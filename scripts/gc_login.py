import os, getpass, garth

def main():
    email = os.getenv("GARMIN_EMAIL") or input("Garmin email: ").strip()
    password = os.getenv("GARMIN_PASSWORD") or getpass.getpass("Garmin password: ")
    # Pokreće 2FA ako je uključeno; pratit će te prompt u terminalu
    garth.login(email, password)
    print("Login OK. Tokens saved to ~/.garth")

if __name__ == "__main__":
    main()
