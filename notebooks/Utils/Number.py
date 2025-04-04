class NumberUtils(object):
    def format_integer_number(self, number: int) -> int:
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