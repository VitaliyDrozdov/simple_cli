class CommandManager:
    def __init__(self):
        self.is_running = True
        self._commands = {
            "SET": 0,
            "GET": 0,
            "UNSET": 0,
            "COUNTS": 0,
            "FIND": 0,
            "BEGIN": 0,
            "ROLLBACK": 0,
            "COMMIT": 0,
            "END": 0,
        }

    def _handle_input(self, line: str):
        if not line:
            return
        line = line.strip().split()
        command = line[0]
        if command not in self._commands:
            print(f"Неизвестная команда {command}")
            return
        self._commands[command](line)

    def run(self):
        while self.is_running:
            try:
                cur_input = input()
            except EOFError:
                print("Программа завершена пользователем.")
                break
            self._handle_input(cur_input)


if __name__ == "__main__":
    app = CommandManager()
    app.run()
