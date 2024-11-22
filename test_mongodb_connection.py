import os
from dotenv import load_dotenv
import pymongo
import certifi
import socket
import dns.resolver
import requests

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

def check_dns():
    try:
        # First test with a known domain
        print("\nTesting DNS with google.com...")
        google_answers = dns.resolver.resolve('google.com', 'A')
        print("Google.com DNS Resolution successful:")
        for rdata in google_answers:
            print(f"Google IP Address: {rdata}")

        print("\nChecking MongoDB Atlas DNS resolution...")
        # Try resolving the MongoDB hostname
        try:
            answers = dns.resolver.resolve('cluster0.gui95.mongodb.net', 'A')
            print("MongoDB DNS Resolution successful:")
            for rdata in answers:
                print(f"MongoDB IP Address: {rdata}")
        except dns.resolver.NXDOMAIN:
            print("MongoDB domain does not exist. Please verify the hostname.")
            return False
        except dns.resolver.NoAnswer:
            # Try SRV record instead
            print("Trying SRV record resolution...")
            try:
                srv_answers = dns.resolver.resolve('_mongodb._tcp.cluster0.gui95.mongodb.net', 'SRV')
                print("MongoDB SRV Resolution successful:")
                for rdata in srv_answers:
                    print(f"Target: {rdata.target}, Port: {rdata.port}, Priority: {rdata.priority}")
                return True
            except Exception as srv_e:
                print(f"SRV record resolution failed: {str(srv_e)}")
                return False
        return True
    except Exception as e:
        print(f"DNS Resolution failed: {str(e)}")
        print("\nChecking current DNS servers...")
        try:
            with open('/etc/resolv.conf', 'r') as f:
                print(f.read())
        except:
            print("Could not read DNS configuration")
        return False

def test_connection():
    try:
        # First check DNS
        if not check_dns():
            print("\nDNS resolution failed. Possible causes:")
            print("1. Network connectivity issues")
            print("2. DNS server problems")
            print("3. Firewall blocking DNS queries")
            print("4. VPN or proxy interference")
            print("\nTrying to verify internet connectivity...")
            try:
                response = requests.get("https://www.google.com", timeout=5)
                print("Internet connection is working (can reach google.com)")
            except:
                print("Cannot reach google.com - possible network connectivity issue")
            return

        print("\nAttempting to connect to MongoDB...")
        # Parse the connection string to get host and port
        if "mongodb+srv://" in MONGO_DB_URL:
            connection_parts = MONGO_DB_URL.split('@')[1].split('/')[0]
            host = connection_parts
            port = 27017
        else:
            print("Invalid connection string format")
            return

        print(f"Testing TCP connection to {host}:{port}...")
        # Try TCP connection first
        sock = socket.create_connection((host, port), timeout=10)
        print("TCP Connection successful!")
        sock.close()

        # Now try MongoDB connection
        print("\nTesting MongoDB connection...")
        client = pymongo.MongoClient(
            MONGO_DB_URL,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=30000,
            ssl=True,
            retryWrites=True,
            connectTimeoutMS=20000,
            socketTimeoutMS=20000,
            maxPoolSize=1,
            waitQueueTimeoutMS=30000
        )
        
        print("Client created, testing database access...")
        db = client.get_database("JSEGSecurity")
        db.command("ping")
        print("Successfully connected to MongoDB!")
        
        print("\nAvailable databases:")
        dbs = client.list_database_names()
        for db in dbs:
            print(f"- {db}")
            
    except socket.error as e:
        print(f"TCP Connection failed: {str(e)}")
        print("This suggests a network connectivity issue or firewall blocking the connection.")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(f"Server Selection Timeout Error: {str(e)}")
        print("This might be due to network latency or firewall issues.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Connection Failure: {str(e)}")
    except pymongo.errors.OperationFailure as e:
        print(f"Operation Failure (possibly auth related): {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(f"Error type: {type(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    test_connection()
