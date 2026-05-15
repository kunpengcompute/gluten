//
// Created by root on 4/29/25.
//

#include "OmniPlanConverter.h"

namespace omniruntime
{
OmniPlanConverter::OmniPlanConverter(const std::vector<std::shared_ptr<ResultIterator>> &inputIters,
    mem::MemoryPool *OmniPool, const std::unordered_map<std::string, std::string> &confMap,
    const std::optional<std::string> writeFilesTempPath, bool validationMode)
    : validationMode_(validationMode),
      substraitOmniPlanConverter_(confMap, writeFilesTempPath, validationMode)
{
    substraitOmniPlanConverter_.setInputIters(std::move(inputIters));
}

std::shared_ptr<IcebergSplitInfo> parseIcebergSplitInfo(
    const substrait::ReadRel_LocalFiles_FileOrFiles& file,
    std::shared_ptr<SplitInfo> splitInfo)
{
    using SubstraitFileFormatCase =
        ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::FileFormatCase;
    using SubstraitDeleteFileFormatCase =
        ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::DeleteFile::FileFormatCase;

    auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(splitInfo);
    if (icebergSplitInfo == nullptr) {
        icebergSplitInfo = std::make_shared<IcebergSplitInfo>(*splitInfo);
    }

    auto icebergReadOption = file.iceberg();
    switch (icebergReadOption.file_format_case()) {
        case SubstraitFileFormatCase::kParquet:
            icebergSplitInfo->format = FileFormat::PARQUET;
            break;
        case SubstraitFileFormatCase::kOrc:
            icebergSplitInfo->format = FileFormat::ORC;
            break;
        default:
            icebergSplitInfo->format = FileFormat::UNKNOWN;
            break;
    }

    std::vector<IcebergDeleteFileInfo> deletes;
    deletes.reserve(icebergReadOption.delete_files_size());
    for (int i = 0; i < icebergReadOption.delete_files_size(); i++) {
        auto deleteFile = icebergReadOption.delete_files().Get(i);
        FileFormat deleteFormat;
        switch (deleteFile.file_format_case()) {
            case SubstraitDeleteFileFormatCase::kParquet:
                deleteFormat = FileFormat::PARQUET;
                break;
            case SubstraitDeleteFileFormatCase::kOrc:
                deleteFormat = FileFormat::ORC;
                break;
            default:
                deleteFormat = FileFormat::UNKNOWN;
                break;
        }

        IcebergDeleteContent fileContent;
        switch (deleteFile.filecontent()) {
            case ::substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_POSITION_DELETES:
                fileContent = IcebergDeleteContent::kPositionDeletes;
                break;
            case ::substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_EQUALITY_DELETES:
                fileContent = IcebergDeleteContent::kEqualityDeletes;
                break;
            default:
                fileContent = IcebergDeleteContent::kData;
                break;
        }
        deletes.emplace_back(
            fileContent,
            deleteFormat,
            deleteFile.filepath(),
            deleteFile.recordcount(),
            deleteFile.filesize());
    }
    icebergSplitInfo->deleteFilesVec.emplace_back(std::move(deletes));
    return icebergSplitInfo;
}

std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList)
{
    using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;
    auto splitInfo = std::make_shared<SplitInfo>();
    splitInfo->paths.reserve(fileList.size());
    splitInfo->starts.reserve(fileList.size());
    splitInfo->lengths.reserve(fileList.size());
    splitInfo->partitionColumns.reserve(fileList.size());
    splitInfo->metadataColumns.reserve(fileList.size());
    for (const auto& file : fileList) {
        // Expect all Partitions share the same index.
        splitInfo->partitionIndex = file.partition_index();
        std::unordered_map<std::string, std::string> partitionColumnMap;
        for (const auto& partitionColumn : file.partition_columns()) {
            partitionColumnMap[partitionColumn.key()] = partitionColumn.value();
        }
        splitInfo->partitionColumns.emplace_back(partitionColumnMap);
        std::unordered_map<std::string, std::string> metadataColumnMap;
        for (const auto& metadataColumn : file.metadata_columns()) {
            metadataColumnMap[metadataColumn.key()] = metadataColumn.value();
        }
        splitInfo->metadataColumns.emplace_back(metadataColumnMap);
        splitInfo->paths.emplace_back(file.uri_file());
        splitInfo->starts.emplace_back(file.start());
        splitInfo->lengths.emplace_back(file.length());
        FileProperties fileProps;
        if (file.has_properties()) {
            fileProps.fileSize = file.properties().filesize();
            fileProps.modificationTime = file.properties().modificationtime();
        }
        splitInfo->properties.emplace_back(fileProps);
        switch (file.file_format_case()) {
            case SubstraitFileFormatCase::kOrc:
                splitInfo->format = FileFormat::ORC;
                break;
            case SubstraitFileFormatCase::kParquet:
                splitInfo->format = FileFormat::PARQUET;
                break;
            case SubstraitFileFormatCase::kIceberg:
                splitInfo = parseIcebergSplitInfo(file, std::move(splitInfo));
                break;
            default:
                splitInfo->format = FileFormat::UNKNOWN;
                break;
        }
    }
    return splitInfo;
}


void parseLocalFileNodes(
    SubstraitToOmniPlanConverter* planConverter,
    std::vector<::substrait::ReadRel_LocalFiles>& localFiles)
{
    std::vector<std::shared_ptr<SplitInfo>> splitInfos;
    splitInfos.reserve(localFiles.size());
    for (int32_t i = 0; i < localFiles.size(); i++) {
        const auto& localFile = localFiles[i];
        const auto& fileList = localFile.items();
        splitInfos.push_back(parseScanSplitInfo(fileList));
    }
    planConverter->setSplitInfos(std::move(splitInfos));
}


std::shared_ptr<const PlanNode> OmniPlanConverter::ToOmniPlan(const ::substrait::Plan &substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles)
{
    if (!validationMode_) {
        parseLocalFileNodes(&substraitOmniPlanConverter_, localFiles);
    }
    auto OmniPlan = substraitOmniPlanConverter_.ToOmniPlan(substraitPlan);
    return OmniPlan;
}
}
