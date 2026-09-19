-- Declaration for the luasocket rock (third-party, ships no types):
-- only the subset the tooling calls.
local record TcpSocket
    settimeout: function(TcpSocket, number)
    connect: function(TcpSocket, string, integer): boolean, string
    send: function(TcpSocket, string): number, string
    receive: function(TcpSocket, string): string, string
    close: function(TcpSocket)
end

local record socket
    tcp: function(): TcpSocket
    gettime: function(): number
    sleep: function(number)
end

return socket
