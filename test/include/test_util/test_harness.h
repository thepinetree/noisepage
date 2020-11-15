#pragma once
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "loggers/loggers_util.h"

namespace noisepage {

class NoisepageTest : public ::testing::Test {
 public:
  NoisepageTest() { LoggersUtil::Initialize(); }

  ~NoisepageTest() override { LoggersUtil::ShutDown(); }
};

}  // namespace noisepage
