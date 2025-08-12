# ChomoSyncer - Build & Install Guide (Windows)

This guide covers **Windows (VS 2022 + vcpkg)** installation and build steps for ChomoSyncer. It assumes a clean environment and walks you through dependencies, configuration, and compilation.

---

## 1. Prerequisites - Windows

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

## 2. Dependencies - Windows Dependencies via vcpkg

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

## 4. Configure & Build - Windows (VS 2022 + vcpkg)

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
