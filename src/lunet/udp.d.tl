local record udp
   bind: function(string, integer): (userdata, string)
   send: function(userdata, string, integer, string)
   recv: function(userdata): (string, string, integer)
   close: function(userdata)
   getsockname: function(userdata): (string, integer, string)
end

return udp
