{ lib
, rustPlatform
, pkg-config
, clang
, cmake
, wrapGAppsHook3
, gtk3
, libayatana-appindicator
, libxkbcommon
, libGL
}:

rustPlatform.buildRustPackage {
  pname = "cloudreve-sync";
  version = "0.1.0";
  src = lib.cleanSource ./.;

  cargoLock.lockFile = ./Cargo.lock;

  nativeBuildInputs = [
    pkg-config
    clang
    cmake
    wrapGAppsHook3
  ];

  buildInputs = [
    gtk3
    libayatana-appindicator
    libxkbcommon
    libGL
  ];

  postInstall = ''
    install -Dm644 assets/cloudreve-sync.desktop $out/share/applications/cloudreve-sync.desktop
    install -Dm644 logo-sync.png $out/share/pixmaps/cloudreve-sync.png
  '';

  meta = {
    description = "Two-way Cloudreve desktop sync client";
    homepage = "https://github.com/MartianInGreen/Linux-Cloudreve-Sync";
    license = lib.licenses.mit;
    mainProgram = "cloudreve-sync";
    platforms = lib.platforms.linux;
  };
}
