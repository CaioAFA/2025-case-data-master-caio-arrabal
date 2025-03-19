class SafraUtils(object):
    def get_next_safras(self, safra: int, month_qty: int) -> int:
        str_safra = str(safra)
        year, month = int(str_safra[:4]), int(str_safra[4:])

        month -= 1

        month += month_qty

        year += month // 12
        month = month % 12

        month += 1

        formatted_month = f'0{month}' if month < 10 else (month)
        return int(f'{year}{formatted_month}')
