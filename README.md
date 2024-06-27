# Avro Go

## Implemented
### Encoder

### Decoder

## Not implemented
### Encoder
- pointer receivers
- union containing
    - record
    - array
    - map

### Decoder
- aliases
- default
- array of union
- map of union
- union containing
    - record
    - array of
        - record
        - enum
        - array
        - map
        - fixed
        - union
    - map of
        - record
        - enum
        - array
        - map
        - fixed
        - union

#### Union containing
The optional case has been implemented.

Enum and fixed should be able to be implemented without issue as their types are known.
Record, array and map could be implemented by
- Record: map[string]interface{}
- Array: []interface{}
- Map: map[string]interface{}
