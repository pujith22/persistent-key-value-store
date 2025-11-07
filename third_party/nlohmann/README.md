# Vendored nlohmann/json (single header)

This folder is intended to contain the single-header distribution of nlohmann/json at:

- Path to add: `third_party/nlohmann/json.hpp`

You can obtain it in one of the following ways:

1. Copy from Homebrew installation (macOS):

```bash
# Ensure installed
brew install nlohmann-json

# Copy the header into the repository
mkdir -p third_party/nlohmann
cp "$(brew --prefix nlohmann-json)/include/nlohmann/json.hpp" third_party/nlohmann/json.hpp
```

2. Download single header from GitHub releases:

   - <https://github.com/nlohmann/json/releases>
   - Download `json.hpp` and place it at `third_party/nlohmann/json.hpp`

Build will prefer the vendored header when present because we include with:

```cpp
#include "nlohmann/json.hpp"
```

and compile with `-I./third_party` (see build_instruction.txt). If the vendored header is absent, the build falls back to system-installed header paths.
