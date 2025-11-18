{
  description = "SQL entorno de desarrollo";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      pythonEnv = pkgs.python313.withPackages (ps: with ps; [
        pandas
        numpy
        sqlalchemy
        psycopg2
        jupyterlab
        ipykernel
        matplotlib
        scikit-learn
        python-dateutil
        deep-translator
        transformers
        sentencepiece
        sacremoses
        pytorch
        ]);
    in {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          dbeaver-bin
          sqlcmd
          pythonEnv
          postgresql     
          pgloader
        ];
      };
    };
}
