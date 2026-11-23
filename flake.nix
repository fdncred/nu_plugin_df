{
  description = "Nushell plugin for Apache DataFusion dataframes and SQL";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            }
          )
        );

      # Cargo.toml uses TOML 1.1 (multi-line inline tables), which
      # builtins.fromTOML can't parse; Cargo.lock is plain TOML 1.0.
      pname = "nu_plugin_df";
      version =
        (nixpkgs.lib.findFirst (
          p: p.name == pname
        ) (throw "${pname} not in Cargo.lock") (builtins.fromTOML (builtins.readFile ./Cargo.lock)).package)
        .version;

      # Same toolchain as rustup users get: channel and components come from
      # rust-toolchain.toml.
      toolchainFor = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          toolchain = toolchainFor pkgs;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = toolchain;
            rustc = toolchain;
          };
        in
        {
          default = self.packages.${pkgs.stdenv.hostPlatform.system}.nu_plugin_df;

          nu_plugin_df = rustPlatform.buildRustPackage {
            inherit pname version;

            src = pkgs.lib.cleanSource ./.;

            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = [
              pkgs.pkg-config
              rustPlatform.bindgenHook # clang-sys
            ];
            buildInputs = [ pkgs.zstd ];

            # Only the lib tests are expected to pass (see README build notes).
            cargoTestFlags = [ "--lib" ];

            meta = {
              description = "Nushell plugin for Apache DataFusion dataframes and SQL";
              homepage = "https://github.com/fdncred/nu_plugin_df";
              license = pkgs.lib.licenses.mit;
              mainProgram = "nu_plugin_df";
            };
          };
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            ((toolchainFor pkgs).override {
              extensions = [
                "rust-src"
                "rust-analyzer"
              ];
            })
          ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);
    };
}
