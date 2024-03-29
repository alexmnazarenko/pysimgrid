SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OPT_ROOT="$SCRIPT_DIR/opt"
PKG_ROOT="$OPT_ROOT/SimGrid"
VERSION="v3_13"

echo "Create dirs..."
mkdir -p "$PKG_ROOT/src"

echo "Download..."
if [[ ! -f  "$PKG_ROOT/src/simgrid-${VERSION}.tar.gz" ]]; then
  wget -P "$PKG_ROOT/src" https://framagit.org/simgrid/simgrid/-/archive/${VERSION}/simgrid-${VERSION}.tar.gz
fi

echo "Unpack..."
(cd "$PKG_ROOT/src" && tar xzf "$PKG_ROOT/src/simgrid-${VERSION}.tar.gz")

mkdir "$PKG_ROOT/build"
echo "Configure..."
(cd "$PKG_ROOT/build" && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX="$PKG_ROOT" ../src/SimGrid-3.13)
echo "Build..."
(cd "$PKG_ROOT/build" && make -j4 install)
