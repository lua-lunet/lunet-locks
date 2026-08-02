local tested = require("tested")

package.path = "./build/?.lua;" .. package.path

local record HelloWorld
   greet: function(string): string
end

local hello_world = dofile("./build/hello_world.lua") as HelloWorld

tested.test("hello_world.greet", function()
   tested.assert({
      given = 'name = "world"',
      should = 'return "hello, world"',
      expected = "hello, world",
      actual = hello_world.greet("world"),
   })
end)

return tested
