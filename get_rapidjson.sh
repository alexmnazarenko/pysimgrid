SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OPT_ROOT="$SCRIPT_DIR/opt"
PKG_ROOT="$OPT_ROOT/rapidjson"

echo "Create dirs..."
mkdir -p "$PKG_ROOT/src"

echo "Download..."
if [[ ! -f  "$PKG_ROOT/src/v1.0.2.tar.gz" ]]; then
  wget -P "$PKG_ROOT/src" https://github.com/miloyip/rapidjson/archive/v1.0.2.tar.gz
fi

echo "Unpack..."
(cd "$PKG_ROOT/src" && tar xzf "$PKG_ROOT/src/v1.0.2.tar.gz")

mkdir "$PKG_ROOT/build"
echo "Configure..."
(cd "$PKG_ROOT/build" && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX="$PKG_ROOT" ../src/rapidjson-1.0.2)
echo "Build..."
(cd "$PKG_ROOT/build" && make -j12 install)
