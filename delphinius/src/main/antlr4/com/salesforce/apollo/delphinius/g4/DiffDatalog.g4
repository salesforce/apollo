// Grammar for parsing programs written in (a subset of the) Souffle Datalog language.
// https://souffle-lang.github.io
// This grammar is written in the syntax of the Python Parglare parser generator.

grammar DiffDatalog;

program: declarationList
       ;

declarationList: EMPTY
               | declaration declarationList
               ;
EMPTY: ;


declaration: typeDecl
           | inputDecl
           | outputDecl
           | relationDecl
           | clause
           | init
           | fact
           | component
           | functorDecl
           | compinit
           | pragma
           | override
           ;

override: OVERRIDE Identifier
        ;


functorDecl: FUNCTOR Identifier '(' identifierList ')' ':' Identifier
    ;

init: INIT Identifier '=' compType
    ;

compinit: INSTANTIATE Identifier '=' compType
        ;

compType: Identifier typeparameters
        ;

typeparameters: EMPTY
              | '<' typeparameterList '>'
              ;

typeparameterList: TypeId
                 | TypeId ',' typeparameterList
                 ;

TypeId: Identifier
      | TypeId DOT Identifier
      ; 


pragma: PRAGMA String String
      | PRAGMA String
      ;

componentType: Identifier typeparameters
             ;

component: COMP componentType componentBody
         | COMP componentType ':' componentTypeList componentBody
         ;

componentTypeList : componentType
                  | componentType ',' componentTypeList
                  ;

componentBody: '{' declarationList '}'
             ;

typeDecl: TYPE Identifier
        | TYPE Identifier '<:' predefinedType
        | TYPE Identifier '=' unionType
        | TYPE Identifier '=' sumBranchList
        | TYPE Identifier '=' '[' ']'
        | TYPE Identifier '=' '[' recordType ']'
        | SYMBOL_TYPE Identifier
        | NUMBER_TYPE Identifier
        ;

sumBranchList: sumBranch
             | sumBranch '|' sumBranchList
             ;

sumBranch: Identifier '{' '}'
         | Identifier '{' recordType '}'
         ;

predefinedType: TNUMBER
              | TFLOAT
              | TSYMBOL
              | TUNSIGNED
              ;

unionType : TypeId
          | TypeId '|' unionType
          ;

recordType: Identifier ':' TypeId
          | Identifier ':' TypeId ',' recordType
          ;

relationDecl: DECL relationList relationBody qualifiers dependencyList
            ;

dependencyList: EMPTY
            | CHOICEDOMAIN dependencyListAux
            ;

dependencyListAux: dependency
                 | dependencyListAux ',' dependency
                 ;

dependency: Identifier
          | '(' nonEmptyidentifierList ')'
          ;

relationList: Identifier
            | Identifier ',' relationList
            ;

relationBody: '(' parameterList ')'
            ;

qualifiers: EMPTY
          | qualifier qualifiers
          ;

qualifier: OUTPUT_QUALIFIER
         | INPUT_QUALIFIER
         | PRINTSIZE_QUALIFIER
         | OVERRIDABLE_QUALIFIER
         | INLINE_QUALIFIER
         | BRIE_QUALIFIER
         | BTREE_QUALIFIER
         | EQREL_QUALIFIER
         ;

kvValue : String
        | 'True'
        | Identifier
        ;

keyValuePairs : EMPTY
              | Identifier '=' kvValue
              | Identifier '=' kvValue ',' keyValuePairs
              ;

// load_head in the souffle grammar
inputDecl: INPUT iodirectiveList
         ;

// store_head in the souffle grammar
outputDecl: OUTPUT iodirectiveList
          | PRINTSIZE iodirectiveList
          ;

iodirectiveList : iorelationList
                | iorelationList '(' keyValuePairs ')'
                ;

iorelationList : relId
               | relId ',' iorelationList
               ;

parameterList: parameter ',' parameterList
             | parameter
             | EMPTY
             ;

parameter: Identifier ':' Identifier
         ;

identifierList: EMPTY
              | identifierList ',' Identifier
              ;

nonEmptyidentifierList: Identifier
              | nonEmptyidentifierList ',' Identifier
              ;

argumentList: argument
            | argument ',' argumentList
            ;

argument: Identifier '=' String
        ;

fact: atom DOT
    ;

head: atom
    | head ',' atom
    ;

term: literal
    | '!' term
    | '(' disjunction ')'
    ;

disjunction: conjunction
           | conjunction OR disjunction
           ;

conjunction: term
           | term ',' conjunction
           ;

arg: String
   | '_'
   | '$'
   | '@' Identifier functorList
   | Identifier
   | FLOAT
   | IntegerLiteral
   | '(' arg ')'
   | arg 'lor' arg  {0, left}
   | arg 'land' arg {1, left}
   | arg 'bor' arg  {2, left}
   | arg 'bxor' arg {3, left}
   | arg 'band' arg {4, left}
   | arg '+' arg    {5, left}
   | arg '-' arg    {5, left}
   | arg '*' arg    {6, left}
   | arg '/' arg    {6, left}
   | arg '%' arg    {6, left}
   | arg '^' arg    {9, right}
   | functionCall
   | arg AS Identifier
   | 'bnot' arg     {7}
   | 'lnot' arg     {7}
   | '-' arg        {8}
   | '[' ']'
   | '[' recordList ']'
   | aggregate
   | NIL
   | '$' Identifier '(' argList ')'
   ;

functorList : '(' ')'
            | '(' functorArgs ')'
            ;

functorArgs : arg
            | arg ',' functorArgs
            ;

recordList : arg
           | arg ',' recordList
           ;

argList : EMPTY
        | arg
        | arg ',' argList
        ;

atom : relId '(' argList ')'
     ;

relId : Identifier
      | Identifier DOT relId
      ;

Attributes : EMPTY
           | Identifier ':' TypeId
           | Identifier ':' TypeId ',' Attributes
           ;

literal : arg relop arg
        | atom
        | 'match' '(' arg ',' arg ')'
        | 'contains' '(' arg ',' arg ')'
        | TRUE
        | FALSE
        ;

relop: '<' | '>' | '=' | '!=' | '>=' | '<=' ;

aggregate : 'min' arg ':' aggregateBody
          | 'max' arg ':' aggregateBody
          | 'sum' arg ':' aggregateBody
          | 'mean' arg ':' aggregateBody
          | 'count' ':' aggregateBody
          ;

aggregateBody :  atom
              // | '{' body '}' // TODO
              | '{' conjunction '}'
              ;

body : conjunction
     | conjunction OR body;

functionCall: 'min' '(' functionargumentList ')'
            | 'max' '(' functionargumentList ')'
            | 'cat' '(' functionargumentList ')'
            | 'ord' '(' functionargumentList ')'
            | 'strlen' '(' functionargumentList ')'
            | 'tonumber' '(' functionargumentList ')'
            | 'tostring' '(' functionargumentList ')'
            | 'substr' '(' functionargumentList ')'
            | 'match' '(' functionargumentList ')'
            | 'contains' '(' functionargumentList ')'
            | 'to_string' '(' functionargumentList ')'
            | 'to_number' '(' functionargumentList ')'
            | 'to_float' '(' functionargumentList ')'
            | 'to_unsigned' '(' functionargumentList ')'
            | 'ftoi' '(' functionargumentList ')'
            | 'itof' '(' functionargumentList ')'
            | 'itou' '(' functionargumentList ')'
            | 'utoi' '(' functionargumentList ')'
            | 'utof' '(' functionargumentList ')'
            | 'ftou' '(' functionargumentList ')'
            | 'sin' '(' functionargumentList ')'
            | 'cos' '(' functionargumentList ')'
            | 'tan' '(' functionargumentList ')'
            | 'asin' '(' functionargumentList ')'
            | 'acos' '(' functionargumentList ')'
            | 'atan' '(' functionargumentList ')'
            | 'sinh' '(' functionargumentList ')'
            | 'cosh' '(' functionargumentList ')'
            | 'tanh' '(' functionargumentList ')'
            | 'asinh' '(' functionargumentList ')'
            | 'acosh' '(' functionargumentList ')'
            | 'atanh' '(' functionargumentList ')'
            | 'log' '(' functionargumentList ')'
            | 'exp' '(' functionargumentList ')'
            | 'range' '(' functionargumentList ')'
            ;

functorCall: '@' Identifier '(' functionargumentList ')'
           ;

functionargumentList: arg
                    | arg ',' functionargumentList
                    ;

plan_or_Empty: EMPTY
             | plan
             ;

plan: PLAN planList
    ;

planList: planNumList
        | planNumList ',' planList
        ;

planNumList: IntegerLiteral ':' '(' numberList ')'
           ;

numberList: IntegerLiteral
          | IntegerLiteral ',' numberList
          ;

clause : head ':-' body DOT plan_or_Empty
     ;

////// Special stuff

LAYOUT: LayoutItem
      | LAYOUT LayoutItem
      ;

LayoutItem: WS
          | Comment
          | MULTILINE_COMMENT
          | EMPTY
          ;

CorNCs: CorNC | CorNCs CorNC | EMPTY;
CorNC: Comment | NotComment | WS;
Comment: '/*' CorNCs '*/' ;

// terminals
DOT: '.' ;
IO: 'IO' ;
FILE: '\'file\'' ;
FILENAME: 'filename' ;
DELIMITER: 'delimiter' ;
TYPE: '.type' ;
SYMBOL_TYPE: '.symbol_type' ;
NUMBER_TYPE: '.number_type' ;
FUNCTOR: '.functor' ;
INIT: '.init' ;
COMP: '.comp' ;
DECL: '.decl' ;
INPUT: '.input' ;
OUTPUT: '.output' ;
OUTPUT_QUALIFIER: 'output' ;
INPUT_QUALIFIER: 'input' ;
PRINTSIZE_QUALIFIER: 'printsize' ;
OVERRIDABLE_QUALIFIER: 'overridable' ;
INLINE_QUALIFIER: 'inline' ;
BRIE_QUALIFIER: 'brie' ;
BTREE_QUALIFIER: 'btree' ;
EQREL_QUALIFIER: 'eqrel' ;
OR: ';' ; 
String: '"'[^'"']*'"';
INSTANTIATE: 'instantiate' ;
PRAGMA: '.pragma' ;
PRINTSIZE: '.printsize' ;
AS: 'as' ;
NIL: 'nil' ;
TRUE: 'true' ;
FALSE: 'false' ;
FLOAT: ([0-9]+)[.]([0-9]+) ;


Identifier: [_a-zA-Z\?][_\d\w\?]*;
NotComment: (('*'[^\/])|[^\s*\/]|[^\*])+ ;
PLAN: '.plan' ;
OVERRIDE: '.override' ;
TNUMBER: 'number' ;
TSYMBOL: 'symbol' ;
TUNSIGNED: 'unsigned' ;
TFLOAT: 'float' ;
CHOICEDOMAIN: 'choice-domain' ;
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