list_1 = [2, 5, 7]
list_2 = [3, 6, 8, 9]

def find_even_numbers(list):
    for n in list:
        if (n % 2) == 0:
            print(f'{n} is even')

find_even_numbers(list_1)

find_even_numbers(list_2)

def select_second(L):
    """Return the second element of the given list. If the list has no second
    element, return None.
    """
    return L[1]

Lista = (1, 2, 3)
print(select_second(Lista))

def purple_shell(racers):
    """Given a list of racers, set the first place racer (at the front of the list) to last
    place and vice versa.
    
    >>> r = ["Mario", "Bowser", "Luigi"]
    >>> purple_shell(r)
    >>> r
    ["Luigi", "Bowser", "Mario"]
    """
    racers[0], racers[-1] = racers[-1], racers[0]
    return racers

r = ["Mario", "Bowser", "Luigi"]

print(purple_shell(r))