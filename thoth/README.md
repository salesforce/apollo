# Thoth

KERI KERL, Validation, Witnessing, etc, at scale.

---

## DHT Unified KERL

Thoth provides a unified KERL in the form of a _KerlDHT_ , a distributed hash table that is byzantine, fault tolerant
and
highly scalable. The digest of individual KERL identifiers is mapped to the Fireflies rings of the membership context.
Successors of this digest are the storage for that identifier's KERL. When membership changes, a gossip protocol
rebalances any state movement required to rehost identifier KERLs that need to be moved to accommodate the membership
change.

## Ani Key Event Validation

KERI validation functionality is provided by *Ani*. Ani provides the behavior for two kinds of event validation:

* root validation
* KERL set validation

### Root Validation

This *EventValidation* implementation only validates against the *root* identifiers of the *Ani* instance. Likely will
evolve as bootstrap tightens. This validator is used for bootstrapping members into the Apollo *Fireflies* membership.

### KERL Set Validation

This *EventValidation* implementation usse

Thoth also provides validation and witnessing services to provide a full-featured public key infrastructure for the rest
of the Apollo stack.
