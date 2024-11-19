
grammar TrinityLakeSqlExtensions;

singleStatement
    : statement EOF
    ;

statement
    : beginStatement
    | commitStatement
    | rollbackStatement
    ;

beginStatement
    : BEGIN (TRANSACTION)?
    ;

commitStatement
    : COMMIT (TRANSACTION)?
    ;

rollbackStatement
    : ROLLBACK (TRANSACTION)?
    ;

nonReserved
    : BEGIN | COMMIT | ROLLBACK | TRANSACTION
    ;

BEGIN: 'BEGIN';
TRANSACTION: 'TRANSACTION';
COMMIT: 'COMMIT';
ROLLBACK: 'ROLLBACK';

SIMPLE_COMMENT
    : '--' (~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we don't recognize
UNRECOGNIZED
    : .
    ;