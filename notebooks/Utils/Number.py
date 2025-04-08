class NumberUtils(object):
    def format_integer_number(self, number: int) -> str:
        '''
        Transform "100000" into "100.000"
        '''
        str_number = str(number)

        # Reversing to insert points on the correct place
        str_number = list(str_number)
        str_number.reverse()
        str_number = "".join(str_number)

        formatted_number = []
        for index, n in enumerate(str_number):
            formatted_number.append(n)
            if (index + 1) % 3 == 0 and index != len(str_number) - 1:
                formatted_number.append('.')
        
        formatted_number.reverse()
        result = ''.join(formatted_number)
        result = result.replace('-.', '-') # Fix negative values problem
        return result
    

    def format_float_number(self, number: float, decimal_houses: int = None) -> str:
        splitted_num = str(number).split('.')

        int_number = splitted_num[0]
        decimal_digits = splitted_num[1] if len(splitted_num) > 1 else None

        formatted_int_number = self.format_integer_number(int_number)

        result = formatted_int_number
        if decimal_digits:
            if decimal_houses:
                decimal_digits = decimal_digits[:decimal_houses]

            result += ',' + decimal_digits

        return result