open Ocamlbuild_plugin

let () = dispatch (
  function
  | After_rules ->
    flag ["ocaml"; "compile"; "use_protobuf"] &
      S[A"-ppx"; A("ocamlfind ppx_deriving/ppx_deriving src/ppx_deriving_protobuf.cmo")];

  | _ -> ())
