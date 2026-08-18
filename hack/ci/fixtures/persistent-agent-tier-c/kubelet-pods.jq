{
  apiVersion: .apiVersion,
  kind: .kind,
  metadata: .metadata,
  items: [
    .items[]
    | select(any(.spec.containers[]?; .name == "compute-domains"))
  ]
}
