with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "maelstrom-env";
  nativeBuildInputs = [
    openjdk
    graphviz
    gnuplot
    leiningen
    clojure
  ];
  buildInputs = [
    # jdk
    # graphviz
    # gnuplot
  ];

  # Set Environment Variables
  # foo = BAR;
}
