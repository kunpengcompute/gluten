//
// Created by root on 12/26/25.
//

#include "OmniBackend.h"
#include "udf/UdfLoader.h"
#include "config/OmniConfig.h"

namespace gluten {
std::unique_ptr<OmniBackend> OmniBackend::instance_ = nullptr;

void OmniBackend::create(const std::unordered_map<std::string, std::string> &conf)
{
    instance_ = std::unique_ptr<OmniBackend>(new OmniBackend(conf));
}

void OmniBackend::init(const std::unordered_map<std::string, std::string> &conf)
{
    backendConf_ = std::make_shared<omniruntime::config::ConfigBase>(
        std::unordered_map<std::string, std::string>(conf));
    initUdf();
}

void OmniBackend::initUdf() const
{
    auto got = backendConf_->Get<std::string>(omniruntime::kOmniUdfLibraryPaths, "");
    if (!got.empty()) {
        auto udfLoader = UdfLoader::getInstance();
        udfLoader->loadUdfLibraries(got);
        udfLoader->registerUdf();
    }
}
}
