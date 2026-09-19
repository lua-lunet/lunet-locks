local record bit
    tobit: function(number): integer
    ror: function(number, number): integer
    rol: function(number, number): integer
    rshift: function(number, number): integer
    lshift: function(number, number): integer
    arshift: function(number, number): integer
    bxor: function(number, number, ...: number): integer
    bor: function(number, number, ...: number): integer
    band: function(number, number, ...: number): integer
    bnot: function(number): integer
    bswap: function(number): integer
    tohex: function(number): string
end

return bit
