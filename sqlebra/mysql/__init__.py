"""
Cheat sheet:

# First, initialize the mysql shell:
$ sudo mysql -u root

# Create a new user
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';

# Grant privileges
mysql> GRANT ALL PRIVILEGES ON * . * TO 'newuser'@'localhost';

# Reload privileges
mysql> FLUSH PRIVILEGES;
"""

from .mysqldb import MySQLDB
