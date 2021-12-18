/**
 * A Datalog grammar inspired from the textbook definition and heavily taken
 * from http://blogs.evergreen.edu/sosw/files/2014/04/Green-Vol5-DBS-017.pdf
 *
 * As per the semantics mentioned in the above link the grammar below supports
 * negation and aggregation as well. Main differences from the above spec to
 * this is how literals and variables are represented which differ a bit from
 * traditional logic programming convention.
 *
 * All literals are typed in below grammar and variables must be explicitly
 * indicated by '?' prefix. Predicates follow same rules as identifiers except
 * that they don't have the '?' character. These changes are so that grammar
 * can be used to store/query typed data from database.
 */
grammar Datalog;

// Top level program to perform two functions.
// Stores data using facts or performs a query over existing/new facts
program
    : facts
    | facts? rules? query
    ;

// Facts that represent ground truth information
fact
    : predicate '(' literalList ')' '.'
    ;

literalList
    : literal ( ',' literal ) *
    ;

facts
    : fact+
    ;

// Horn clauses or rules in the query
clause
    : atom ':-' atoms '.'
    ;

rules
    : clause+
    ;

query
    : '?-' atom
    ;

// Definition of atom (or goal) used in horn clauses (or rules)
atom
    : predicate '(' variableOrLiterals ')'
    | NOT atom
    ;

atoms
    : atom ( ',' atom )*
    ;

// Types of things allowed within arguments of rules
variableOrLiteral
    : variable
    | literal
    | aggregateVariable
    ;

variableOrLiterals
    : variableOrLiteral (',' variableOrLiteral)*
    ;

aggregateVariable
    : aggregateOp '<' variable '>'
    ;

aggregateOp
    : COUNT
    | AVG
    | SUM
    | MIN
    | MAX
    ;

variable
    : Identifier
    ;

predicate
    : Predicate
    ;

literal
    :   IntegerLiteral
    |   FloatingPointLiteral
    |   BooleanLiteral
    |   CharacterLiteral
    |   StringLiteral
    ;

// Lexer rules

//
// Keywords
//
NOT: 'not';
COUNT: 'count';
AVG: 'avg';
SUM: 'sum';
MIN: 'min';
MAX: 'max';

//
// Integers
//
IntegerLiteral
    :   DecimalIntegerLiteral
    |   HexIntegerLiteral
    |   OctalIntegerLiteral
    |   BinaryIntegerLiteral
    ;

fragment
DecimalIntegerLiteral
    :   DecimalNumeral IntegerTypeSuffix?
    ;

fragment
HexIntegerLiteral
    :   HexNumeral IntegerTypeSuffix?
    ;

fragment
OctalIntegerLiteral
    :   OctalNumeral IntegerTypeSuffix?
    ;

fragment
BinaryIntegerLiteral
    :   BinaryNumeral IntegerTypeSuffix?
    ;

fragment
IntegerTypeSuffix
    :   [lL]
    ;

fragment
DecimalNumeral
    :   '0'
    |   NonZeroDigit (Digits? | Underscores Digits)
    ;

fragment
Digits
    :   Digit (DigitsAndUnderscores? Digit)?
    ;

fragment
Digit
    :   '0'
    |   NonZeroDigit
    ;

fragment
NonZeroDigit
    :   [1-9]
    ;

fragment
DigitsAndUnderscores
    :   DigitOrUnderscore+
    ;

fragment
DigitOrUnderscore
    :   Digit
    |   '_'
    ;

fragment
Underscores
    :   '_'+
    ;

fragment
HexNumeral
    :   '0' [xX] HexDigits
    ;

fragment
HexDigits
    :   HexDigit (HexDigitsAndUnderscores? HexDigit)?
    ;

fragment
HexDigit
    :   [0-9a-fA-F]
    ;

fragment
HexDigitsAndUnderscores
    :   HexDigitOrUnderscore+
    ;

fragment
HexDigitOrUnderscore
    :   HexDigit
    |   '_'
    ;

fragment
OctalNumeral
    :   '0' Underscores? OctalDigits
    ;

fragment
OctalDigits
    :   OctalDigit (OctalDigitsAndUnderscores? OctalDigit)?
    ;

fragment
OctalDigit
    :   [0-7]
    ;

fragment
OctalDigitsAndUnderscores
    :   OctalDigitOrUnderscore+
    ;

fragment
OctalDigitOrUnderscore
    :   OctalDigit
    |   '_'
    ;

fragment
BinaryNumeral
    :   '0' [bB] BinaryDigits
    ;

fragment
BinaryDigits
    :   BinaryDigit (BinaryDigitsAndUnderscores? BinaryDigit)?
    ;

fragment
BinaryDigit
    :   [01]
    ;

fragment
BinaryDigitsAndUnderscores
    :   BinaryDigitOrUnderscore+
    ;

fragment
BinaryDigitOrUnderscore
    :   BinaryDigit
    |   '_'
    ;

//
// Floating point numbers
//
FloatingPointLiteral
    :   DecimalFloatingPointLiteral
    |   HexadecimalFloatingPointLiteral
    ;

fragment
DecimalFloatingPointLiteral
    :   Digits '.' Digits? ExponentPart? FloatTypeSuffix?
    |   '.' Digits ExponentPart? FloatTypeSuffix?
    |   Digits ExponentPart FloatTypeSuffix?
    |   Digits FloatTypeSuffix
    ;

fragment
ExponentPart
    :   ExponentIndicator SignedInteger
    ;

fragment
ExponentIndicator
    :   [eE]
    ;

fragment
SignedInteger
    :   Sign? Digits
    ;

fragment
Sign
    :   [+-]
    ;

fragment
FloatTypeSuffix
    :   [fFdD]
    ;

fragment
HexadecimalFloatingPointLiteral
    :   HexSignificand BinaryExponent FloatTypeSuffix?
    ;

fragment
HexSignificand
    :   HexNumeral '.'?
    |   '0' [xX] HexDigits? '.' HexDigits
    ;

fragment
BinaryExponent
    :   BinaryExponentIndicator SignedInteger
    ;

fragment
BinaryExponentIndicator
    :   [pP]
    ;

//
// Boolean
//
BooleanLiteral
    :   'true'
    |   'false'
    ;

//
// Characters
//
CharacterLiteral
    :   '\'' SingleCharacter '\''
    |   '\'' EscapeSequence '\''
    ;

fragment
SingleCharacter
    :   ~['\\\r\n]
    ;

//
// Strings
//
StringLiteral
    :   '"' StringCharacters? '"'
    ;

fragment
StringCharacters
    :   StringCharacter+
    ;

fragment
StringCharacter
    :   ~["\\\r\n]
    |   EscapeSequence
    ;

// §3.10.6 Escape Sequences for Character and String Literals
fragment
EscapeSequence
    :   '\\' [btnfr"'\\]
    |   OctalEscape
    ;

fragment
OctalEscape
    :   '\\' OctalDigit
    |   '\\' OctalDigit OctalDigit
    |   '\\' ZeroToThree OctalDigit OctalDigit
    ;

fragment
ZeroToThree
    :   [0-3]
    ;

//
// Identifiers & Predicates
//
Predicate
    : IdentifierLetter IdentifierLetterOrDigit*
    ;

Identifier
    : '?' IdentifierLetter IdentifierLetterOrDigit*
    ;

fragment
IdentifierLetter
    : [a-zA-Z]
    ;

fragment
IdentifierLetterOrDigit
    : [a-zA-Z0-9_]
    ;

//
// Whitespace and comments
//
WS
    : [ \t\r\n\u000C]+ -> skip
    ;

COMMENT
    : '%' ~[\n\r]* ( [\n\r] | EOF) -> skip
    ;

MULTILINE_COMMENT
    : '/*' ( MULTILINE_COMMENT | . )*? ('*/' | EOF) -> skip
    ;