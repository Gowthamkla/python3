nos = [1,2,3,4,5]

square_nos = []

square = lambda n:n**2

  for n in nos:
      square_nos.append(square(n))
print(square_nos)