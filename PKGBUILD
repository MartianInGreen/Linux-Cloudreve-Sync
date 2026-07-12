pkgname=cloudreve-sync
pkgver=0.1.0
pkgrel=1
pkgdesc="Two-way Cloudreve desktop sync client"
arch=('x86_64')
url="https://github.com/MartianInGreen/Linux-Cloudreve-Sync"
license=('MIT')
depends=('gtk3' 'libayatana-appindicator' 'libxkbcommon' 'libgl')
makedepends=('cargo' 'clang' 'cmake' 'pkgconf')
source=("${pkgname}-${pkgver}.tar.gz::${url}/archive/refs/tags/v${pkgver}.tar.gz")
sha256sums=('SKIP')

build() {
  cd "Linux-Cloudreve-Sync-${pkgver}"
  cargo build --release --locked
}

check() {
  cd "Linux-Cloudreve-Sync-${pkgver}"
  cargo test --release --locked
}

package() {
  cd "Linux-Cloudreve-Sync-${pkgver}"
  install -Dm755 target/release/cloudreve-sync "${pkgdir}/usr/bin/cloudreve-sync"
  install -Dm644 LICENSE "${pkgdir}/usr/share/licenses/${pkgname}/LICENSE"
  install -Dm644 README.md "${pkgdir}/usr/share/doc/${pkgname}/README.md"
  install -Dm644 assets/cloudreve-sync.desktop "${pkgdir}/usr/share/applications/cloudreve-sync.desktop"
  install -Dm644 logo-sync.png "${pkgdir}/usr/share/pixmaps/cloudreve-sync.png"
}
