from textual.app import App, ComposeResult
from textual.widgets import Label
class HelloWorld(App):
    def compose(self) -> ComposeResult:
        yield Label("Hello Textual")
if __name__ == "__main__":
    app = HelloWorld()
    app.run()
