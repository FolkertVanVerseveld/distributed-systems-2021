from kvstore.s3 import delete_file, list_files


def main():
    for filename in list_files():
        delete_file(filename)

if __name__ == '__main__':
    main()

