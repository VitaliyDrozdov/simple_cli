def require_args(exp_count: int, cmd_name: str):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            if len(*args, **kwargs) != exp_count:
                print(f"Команда {cmd_name} требует {exp_count - 1} аргумента")
                return
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


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
        # if len(self.transactions) <= 1:
        #     return False
        # base = self.transactions[0]
        # for layer in self.transactions[1:]:
        #     for k, v in layer.items():
        #         base[k] = v
        # self.transactions = [base]
        # return True
        # # if len(self.transactions) <= 1:
        # #     return False
        # # last = self.transactions.pop()
        # # for k, v in last.items():
        # #     self.transactions[-1][k] = v
        # # return True
        # if len(self.transactions) <= 1:
        #     return False

        # Объединяем все изменения из всех открытых транзакций (кроме базового)
        merged = {}
        for layer in self.transactions[1:]:
            merged.update(layer)

        # Оставляем только базовый слой
        self.transactions = [self.transactions[0]]
        # Применяем все изменения к базовому слою
        for k, v in merged.items():
            self.transactions[0][k] = v

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
            "BEGIN": self._handle_transaction_cmd,
            "ROLLBACK": self._handle_transaction_cmd,
            "COMMIT": self._handle_transaction_cmd,
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

    @require_args(3, "SET")
    def handle_set(self, user_commands: list[str]):
        k, v = user_commands[1], user_commands[2]
        self.db.set(k, v)

    @require_args(2, "GET")
    def handle_get(self, user_commands: list[str]):
        k = user_commands[1]
        print(self.db.get(k))

    @require_args(2, "UNSET")
    def handle_unset(self, user_commands: list[str]):
        key = user_commands[1]
        self.db.unset(key)

    @require_args(2, "COUNTS")
    def handle_counts(self, user_commands: list[str]):
        value = user_commands[1]
        print(self.db.counts(value))

    @require_args(2, "FIND")
    def handle_find(self, user_commands: list[str]):
        value = user_commands[1]
        print(self.db.find(value))

    def _handle_transaction_cmd(self, cmds: list[str]):
        cmd = cmds[0]
        if len(cmds) != 1:
            print(f"{cmd} не принимает аргументов")
            return
        if cmd == "BEGIN":
            self.db.begin()
        elif cmd == "ROLLBACK":
            if not self.db.rollback():
                # print("NO TRANSACTION")
                pass
        elif cmd == "COMMIT":
            if not self.db.commit():
                # print("NO TRANSACTION")
                pass

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
