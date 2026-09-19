PNGS := $(wildcard *.png)
CHROME ?= /Applications/Google Chrome.app/Contents/MacOS/Google Chrome

.PHONY: diagram clean

# Render NAME.trace to NAME.svg (always) and NAME.png (only if missing; PNGs
# are reused, never regenerated). Rasterization uses headless Chrome when
# available, falling back to ImageMagick.
diagram:
	@test -n "$(NAME)" || { echo "usage: make diagram NAME=<trace name>"; exit 1; }
	uv run msgtrace.py $(NAME).trace
	@if [ -f $(NAME).png ]; then \
		echo "reusing existing $(NAME).png"; \
	elif [ -x "$(CHROME)" ]; then \
		DIMS=$$(sed -n 's/.*width="\([0-9.]*\)" height="\([0-9.]*\)".*/\1x\2/p' $(NAME).svg | head -1); \
		"$(CHROME)" --headless --disable-gpu --hide-scrollbars \
			--default-background-color=FFFFFFFF --window-size=$$DIMS \
			--screenshot=$(NAME).png "file://$$PWD/$(NAME).svg" >/dev/null 2>&1; \
		echo "wrote $(NAME).png (chrome)"; \
	elif command -v magick >/dev/null 2>&1; then \
		magick -background white -density 144 $(NAME).svg $(NAME).png; \
		echo "wrote $(NAME).png (magick)"; \
	else \
		echo "no rasterizer available; $(NAME).svg written without png cache"; \
	fi

clean:
	rm -f *.svg *.png
