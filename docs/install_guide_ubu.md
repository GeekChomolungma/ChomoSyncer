# ChomoSyncer - Build & Install Guide (Ubuntu & Windows)

This guide covers **Ubuntu 20.04 / 22.04 / 24.04** installation and build steps for ChomoSyncer. It assumes a clean environment and walks you through dependencies, configuration, and compilation.

---

## 1. Prerequisites - Ubuntu

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config git
```

---

## 2. Dependencies - Ubuntu

### 2.1 Base Libraries

```bash
sudo apt install -y libssl-dev \
    libboost-system-dev libboost-thread-dev \
    libhiredis-dev \
    nlohmann-json3-dev
```

> On Ubuntu 20, `libhiredis-dev` does **not** include a `hiredisConfig.cmake`.

### 2.2 MongoDB C Driver

```bash
sudo apt install -y libsasl2-dev libbson-1.0-0 libbson-dev libmongoc-1.0-0 libmongoc-dev
```

### 2.3 MongoDB C++ Driver (bsoncxx / mongocxx)

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

---

## 3. Clone the Project

```bash
git clone https://github.com/GeekChomolungma/ChomoSyncer.git
cd ChomoSyncer
```

---

## 4. Configure & Build - Ubuntu

We handle platform differences in `CMakeLists.txt`:

* Linux uses `pkg-config` for `mongocxx/bsoncxx/hiredis` when CONFIG packages are not present.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

The executable will be at `build/ChomoSyncer`.
`config.ini` will be copied into `build/config.ini`.

---

## 5. Run

```bash
cd build
./ChomoSyncer
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
