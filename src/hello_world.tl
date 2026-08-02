local record hello_world
end

function hello_world.greet(name: string): string
   return "hello, " .. name
end

return hello_world
