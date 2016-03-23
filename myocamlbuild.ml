open Ocamlbuild_plugin

let () = dispatch (fun phase ->
  Ocamlbuild_cppo.dispatcher phase;
  match phase with
  | After_rules ->
    flag ["ocaml"; "compile"; "use_protobuf"] &
      S[A"-ppx"; A("ocamlfind ppx_deriving/ppx_deriving src/ppx_deriving_protobuf.cma")];

  | _ -> ())
