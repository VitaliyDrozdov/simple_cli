class Database:
    pass


class CommandManager:
    def __init__(self):
        self.is_running = True
        self._commands = {
            "SET": self.handle_set,
            "GET": self.handle_get,
            "UNSET": self.handle_unset,
            "COUNTS": self.handle_counts,
            "FIND": self.handle_find,
            "BEGIN": self.handle_begin,
            "ROLLBACK": self.handle_rollback,
            "COMMIT": self.handle_commit,
            "END": self.handle_end,
        }
        self.db = Database()

    def handle_input(self, line: str):
        if not line:
            return
        user_commands = line.strip().split()
        key = user_commands[0]
        if key not in self._commands:
            print(f"Неизвестная команда {key}")
            return
        self._commands[key](line)

    def handle_set(self, user_commands: list[str]):
        if len(user_commands) != 3:
            print("SET требует два аргумента: SET <ключ> <значение>")
            return
        k, v = user_commands[1][2]
        self.db.set(k, v)

    def handle_get(self, user_commands: list[str]):
        if len(user_commands) != 2:
            print("GET требует один аргумент: GET <ключ>")
            return
        k = user_commands[1]
        self.db.get(k)

    def handle_unset(self, user_commands: list[str]):
        if len(user_commands) != 2:
            print("UNSET требует один аргумент: UNSET <ключ>")
            return
        key = user_commands[1]
        self.db.unset(key)

    def handle_counts(self, user_commands: list[str]):
        if len(user_commands) != 2:
            print("COUNTS требует один аргумент: COUNTS <значение>")
            return
        value = user_commands[1]
        print(self.db.counts(value))

    def handle_find(self, user_commands: list[str]):
        if len(user_commands) != 2:
            print("FIND требует один аргумент: FIND <значение>")
            return
        value = user_commands[1]
        print(self.db.find(value))

    def handle_begin(self, user_commands: list[str]):
        if len(user_commands) != 1:
            print("BEGIN не принимает аргументов")
            return
        self.db.begin()

    def handle_rollback(self, user_commands: list[str]):
        if len(user_commands) != 1:
            print("Ошибка: ROLLBACK не принимает аргументов")
            return
        if not self.db.rollback():
            print("NO TRANSACTION")

    def handle_commit(self, user_commands: list[str]):
        if len(user_commands) != 1:
            print("Ошибка: COMMIT не принимает аргументов")
            return
        if not self.db.commit():
            print("NO TRANSACTION")

    def handle_end(self, user_commands: list[str]):
        if len(user_commands) != 1:
            print("Ошибка: END не принимает аргументов")
            return
        self.running = False

    def run(self):
        while self.is_running:
            try:
                cur_input = input()
            except EOFError:
                print("Программа завершена пользователем.")
                break
            self.handle_input(cur_input)


if __name__ == "__main__":
    app = CommandManager()
    app.run()
