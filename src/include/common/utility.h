#pragma once
#include <string>

namespace noisepage {

int NoisepageClose(int fd);

std::string NoisepageErrorMessage();
}  // namespace noisepage
