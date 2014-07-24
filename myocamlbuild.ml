open Ocamlbuild_plugin

let () = dispatch (
  function
  | After_rules ->
    flag ["ocaml"; "compile"; "use_protobuf"] & S[A"-ppx"; A("src/ppx_protobuf.native")];
    flag ["ocaml"; "compile"; "safe_string"] & A"-safe-string"

  | _ -> ())
