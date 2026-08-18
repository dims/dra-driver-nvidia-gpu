[
  .items[]
  | select(.spec.computeDomainUID == $uid)
  | select(
      .status.phase != "Active" or
      (.status.members | length) != $members or
      ([.status.assignments[]? | select(.state == "Bound")] | length) != $members
    )
]
| length
