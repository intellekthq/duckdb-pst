# boost for x64-windows-static-release uses /MT, but we need /MD
set(VCPKG_TARGET_ARCHITECTURE x64)
# ..., here
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_BUILD_TYPE release)
