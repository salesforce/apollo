# Delphinius
Access Control
____
Apollo Delphinius is a Role Based Access Control system implemented in SQL.

## Model
Delphinius presents a simple model for Access Control Lists.  An ACL is an asserted Tuple of {Object, Relation, Subject}.  Each of the tuple elements forms a seperate domain class.  Each domain is further qualified with Namespaces.  For example an Object is a tuple of {Namespace, Name}. Subject is likewise a tuple of {Namespace, Name}.

Each domain class is further arranged in a directed graphs of mappings. These mappings provide the mechanism to form containment sets.  For example, we can define the Subject of {"foo", "Users"}.  We can map another Subject {"foo", "Jale"} to {"foo", "Users"}.  This could indicate that Subject {"foo", "Jale"} is a "member" of the group {"foo", "Users"}.  While in this example, all the Subjects share the same namespace "foo", this is not required, and namespaces can be interlinked as required.

Note that all domains - Object, Relation and Subject - are both Namespaced and hierarchically related.  While this does allow for bewildering complexity, it is a natural and powerful model that allows concise modeling of rich access control patterns.
## API
The Oracle class provides the following API:
 * add(T) where T in {Object, Relation, Subject, Tuple) - Add the entity
 * delete(T) where T in {Object, Relation, Subject, Tuple) - Delete the entity
 * map(T, T) where T in {Object, Relation, Subject} - Map entity A to entity B
 * check(Tuple) - Check if the Tuple exists

## Design
Delphinius is implemented as a set of SQL tables.  Internally, the full closure sets of all the Domain DAGs are stored in one table - Edge. This strategy trades space for speed, as it is expected that the vast majority of operations performed will be Tuple checks.  As such, Delphinius has a practical upper bound, as the DAG closure table blowout is exponential.  The SQL to implement Delphinius is generic and should work on any other system, but translating the triggers and stored procedures used would have to be accomplished, so it's not a generic component, rather specialized for Apollo's use of the H2DB.

The system is designed to be used to implement row level security in the larger Apollo ecosystem, as well as smoothly integrating with Stereotomy identity and key management.