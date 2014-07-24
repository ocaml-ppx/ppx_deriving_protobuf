#!/usr/bin/env ocaml
#directory "pkg"
#use "topkg.ml"

let () =
  Pkg.describe "ppx_protobuf" ~builder:`OCamlbuild [
    Pkg.lib "pkg/META";
    Pkg.bin ~auto:true "src/ppx_protobuf" ~dst:"../lib/ppx_protobuf/ppx_protobuf";
    Pkg.lib ~exts:Exts.module_library "src/protobuf";
    Pkg.doc "README.md";
    Pkg.doc "LICENSE.txt"; ]
