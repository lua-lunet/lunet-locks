local record Shapes
    record Point
        x: integer
        y: integer
    end

    type Map = {string: Point}
end

local points: Shapes.Map = {
    a = {x = 1, y = 2},
    b = {x = 3, y = 4},
}

print(points)
