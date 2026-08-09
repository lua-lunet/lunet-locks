local record udp
    bind: function(string, integer): (any, string | nil)
    send: function(any, string, integer, string): (boolean | nil, string | nil)
    recv: function(any): (string | nil, string | nil, integer | nil)
    close: function(any): (boolean | nil, string | nil)
end

return udp
