
from datetime import datetime

from helper import write, filter_data
from crawler import scrape


def main():

    new_data = scrape()
    filtered_data = filter_data(new_data)
    write(filtered_data)


if __name__ == '__main__':
    main()