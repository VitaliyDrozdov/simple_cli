class Database:
    def __init__(self):
        self.transactions = [dict()]

    def set(self, key: str, value: str):
        self.transactions[-1][key] = value

    def get(self, key: str) -> str:
        value = None
        for layer in reversed(self.transactions):
            if key in layer:
                value = layer[key]
                break
        return value if value is not None else "NULL"

    def unset(self, key: str):
        self.transactions[-1][key] = None

    def counts(self, value: str) -> int:
        return sum(1 for v in self._build_final_state().values() if v == value)

    def find(self, value: str) -> str:
        keys = [k for k, v in self._build_final_state().items() if v == value]
        return " ".join(keys) if keys else "NULL"

    def begin(self):
        self.transactions.append(dict())

    def rollback(self) -> bool:
        if len(self.transactions) <= 1:
            return False
        self.transactions.pop()
        return True

    def commit(self) -> bool:
        if len(self.transactions) <= 1:
            return False
        current_transaction = self.transactions.pop()
        for k, v in current_transaction.items():
            self.transactions[-1][k] = v
        return True

    def _build_final_state(self) -> dict:
        state = dict()
        for layer in self.transactions:
            for k, v in layer.items():
                state[k] = v
        return {k: v for k, v in state.items() if v is not None}


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
        self._commands[key](user_commands)

    def handle_set(self, user_commands: list[str]):
        if len(user_commands) != 3:
            print("SET требует два аргумента: SET <ключ> <значение>")
            return
        k, v = user_commands[1], user_commands[2]
        self.db.set(k, v)

    def handle_get(self, user_commands: list[str]):
        if len(user_commands) != 2:
            print("GET требует один аргумент: GET <ключ>")
            return
        k = user_commands[1]
        print(self.db.get(k))

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
        self.is_running = False

    def run(self):
        while self.is_running:
            try:
                cur_input = input(">")
            except EOFError:
                print("Программа завершена пользователем.")
                break
            self.handle_input(cur_input)


if __name__ == "__main__":
    app = CommandManager()
    app.run()
