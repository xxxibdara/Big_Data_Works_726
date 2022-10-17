import sys, random


def main(n):
    """
    Estimate Euler's constant by doing n samples from the random variable.
    """
    count = 0
    for i in range(n):
        s = 0.0
        while s < 1:
            s += random.random()
            count += 1
    print(count/n)
    

if __name__ == '__main__':
    n = int(sys.argv[1])
    main(n)