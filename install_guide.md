# ChomoSyncer - Build & Install Guide (Ubuntu & Windows)

This guide covers **Ubuntu 20.04 / 22.04 / 24.04** and **Windows (VS 2022 + vcpkg)** installation and build steps for ChomoSyncer. It assumes a clean environment and walks you through dependencies, configuration, and compilation.

---

## 1. Prerequisites

### 1.1 Ubuntu

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config git
```

### 1.2 Windows

* Install **Visual Studio 2022** with the workload:

  * **Desktop development with C++** (MSVC, CMake, Windows SDK)
* Install **CMake** (if you prefer CLI outside VS): [https://cmake.org/download/](https://cmake.org/download/)
* Install **vcpkg** (recommended on a fixed drive path, e.g., `D:/vcpkg`):

```powershell
# PowerShell (no admin required)
cd D:/
git clone https://github.com/microsoft/vcpkg.git
D:/vcpkg/bootstrap-vcpkg.bat
```

> If you place vcpkg elsewhere, adjust paths below accordingly.

---

## 2. Dependencies

### 2.1 Ubuntu Base Libraries

```bash
sudo apt install -y libssl-dev \
    libboost-system-dev libboost-thread-dev \
    libhiredis-dev \
    nlohmann-json3-dev
```

> On Ubuntu 20, `libhiredis-dev` does **not** include a `hiredisConfig.cmake`.

### 2.2 Ubuntu MongoDB C Driver

```bash
sudo apt install -y libsasl2-dev libbson-1.0-0 libbson-dev libmongoc-1.0-0 libmongoc-dev
```

### 2.3 Ubuntu MongoDB C++ Driver (bsoncxx / mongocxx)

**Option A: From APT (Ubuntu 22.04+)**

```bash
sudo apt install -y libbsoncxx-dev libmongocxx-dev
```

**Option B: From Source (Ubuntu 20.x or older)**

```bash
mkdir -p ~/src && cd ~/src
# download from https://github.com/mongodb/mongo-cxx-driver/releases
# assume tar unpacked to mongo-cxx-driver-rX.Y.Z
cd mongo-cxx-driver-rX.Y.Z/build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
make -j"$(nproc)"
sudo make install
sudo ldconfig
```

Verify:

```bash
pkg-config --modversion libmongocxx
pkg-config --modversion libbsoncxx
```

If installed under `/usr/local`, set `PKG_CONFIG_PATH`:

```bash
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}
```

### 2.4 Windows Dependencies via vcpkg

Make sure `vcpkg` is bootstrapped (see §1.2). Then install these ports:

```powershell
D:/vcpkg/vcpkg.exe install ^
    boost-system boost-thread ^
    openssl ^
    hiredis ^
    nlohmann-json ^
    mongo-cxx-driver
```

Notes:

* `mongo-cxx-driver` will pull `mongo-c-driver` automatically.
* If you target **x64**, you can be explicit:

  ```powershell
  D:/vcpkg/vcpkg.exe install --triplet x64-windows ^
      boost-system boost-thread openssl hiredis nlohmann-json mongo-cxx-driver
  ```
* If you use static runtime, choose `x64-windows-static` accordingly.

---

## 3. Clone the Project

```bash
git clone https://github.com/GeekChomolungma/ChomoSyncer.git
cd ChomoSyncer
```

---

## 4. Configure & Build

### 4.1 Ubuntu

We handle platform differences in `CMakeLists.txt`:

* Linux uses `pkg-config` for `mongocxx/bsoncxx/hiredis` when CONFIG packages are not present.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

The executable will be at `build/ChomoSyncer`.
`config.ini` will be copied into `build/config.ini`.

### 4.2 Windows (VS 2022 + vcpkg)

Use **vcpkg toolchain** when configuring with CMake:

```powershell
cmake -B build -S . ^
  -DCMAKE_TOOLCHAIN_FILE=D:/vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake ^
  -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Alternatively, open the folder with **Visual Studio 2022** (CMake project) and set the CMake toolchain file in:

```
CMakeSettings.json → "toolchainFile": "D:/vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake"
```

Then build the **Release x64** configuration.

The executable will be under:

```
build/Release/ChomoSyncer.exe  (multi-config generators)
```

`config.ini` is copied to the build directory automatically; edit it with your Binance, MongoDB, and Redis settings.

---

## 5. Run

### Ubuntu

```bash
cd build
./ChomoSyncer
```

### Windows

```powershell
cd build/Release
./ChomoSyncer.exe
```

---

## 6. Dependency Summary

| Dependency            | Ubuntu Method                           | Windows (vcpkg)                | Notes                    |
| --------------------- | --------------------------------------- | ------------------------------ | ------------------------ |
| OpenSSL               | `apt install libssl-dev`                | `openssl`                      | TLS for HTTPS/WSS        |
| Boost (system/thread) | `apt install libboost-*`                | `boost-system`, `boost-thread` | Threading, async         |
| hiredis               | `apt install libhiredis-dev`            | `hiredis`                      | Linux may use pkg-config |
| nlohmann/json         | `apt install nlohmann-json3-dev`        | `nlohmann-json`                | Header-only              |
| Mongo C Driver        | `apt install libmongoc-dev libbson-dev` | (pulled by mongo-cxx-driver)   | Required by C++ driver   |
| bsoncxx/mongocxx      | apt (22.04+) or source build            | `mongo-cxx-driver`             | C++ driver for MongoDB   |

---

With these steps, you should be able to build and run **ChomoSyncer** on both Ubuntu and Windows.
