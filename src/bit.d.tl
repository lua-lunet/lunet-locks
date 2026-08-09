local record bit
    tobit: function(number): integer
    ror: function(number, number): integer
    rshift: function(number, number): integer
    bxor: function(number, number, ...: number): integer
    band: function(number, number, ...: number): integer
    bnot: function(number): integer
    tohex: function(number): string
end

return bit
