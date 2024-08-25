{ lib
, rustPlatform
,
}:
rustPlatform.buildRustPackage {
  name = "matrix-migrate";

  src = lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
    allowBuiltinFetchGit = true;
  };

  dontCheck = true;
  dontTest = true;
  dontDoc = true;
  
  meta ={
    description = "";
    homepage = "https://github.com/shymega/matrix-migrate";
  };
}
