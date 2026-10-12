
import asyncio
import aiohttp
import csv
from urllib.parse import urlparse
from collections import defaultdict

INPUT_FILE = "urls.txt"
CONCURRENCY = 20
TIMEOUT_SECONDS = 10

FIELDS = ["url", "status", "status_code", "final_url", "error"]


def normalize_url(raw):
    url = raw.strip()

    if not url:
        return None

    if "://" not in url:
        url = "https://" + url

    try:
        parsed = urlparse(url)
        host = parsed.hostname

        if parsed.scheme not in ("http", "https") or not host:
            return None

        # Validate hostname, including empty labels and extra dots.
        ascii_host = host.encode("idna").decode("ascii")
        labels = ascii_host.rstrip(".").split(".")

        if (
            not ascii_host
            or len(ascii_host.rstrip(".")) > 253
            or any(not label or len(label) > 63 for label in labels)
            or any(
                not (c.isalnum() or c == "-")
                for label in labels
                for c in label
            )
            or any(
                label.startswith("-") or label.endswith("-")
                for label in labels
            )
        ):
            return None

        return url

    except (ValueError, UnicodeError):
        return None


async def check_url(session, semaphore, url):
    async with semaphore:
        try:
            async with session.get(
                url,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(
                    total=TIMEOUT_SECONDS
                ),
            ) as response:
                code = response.status

                if 200 <= code < 400:
                    status = "RUNNING"
                elif code in (401, 403):
                    status = "AUTH_REQUIRED"
                elif code == 404:
                    status = "NOT_FOUND"
                else:
                    status = "HTTP_ERROR"

                return {
                    "url": url,
                    "status": status,
                    "status_code": code,
                    "final_url": str(response.url),
                    "error": "",
                }

        except asyncio.TimeoutError:
            status, error = "TIMEOUT", "Request timed out"

        except aiohttp.ClientConnectorCertificateError:
            status, error = (
                "CERTIFICATE_ERROR",
                "TLS certificate verification failed",
            )

        except aiohttp.ClientConnectorDNSError:
            status, error = "DNS_ERROR", "DNS resolution failed"

        except (UnicodeError, ValueError):
            status, error = "INVALID_URL", "Invalid URL or hostname"

        except aiohttp.ClientError as exc:
            status, error = "CONNECTION_ERROR", type(exc).__name__

        except Exception as exc:
            # One unexpected error should not stop other checks.
            status, error = "CHECK_ERROR", type(exc).__name__

        return {
            "url": url,
            "status": status,
            "status_code": "",
            "final_url": "",
            "error": error,
        }


def save_csv(filename, rows):
    with open(
        filename, "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


async def main():
    with open(INPUT_FILE, encoding="utf-8-sig") as file:
        raw_urls = [line.strip() for line in file if line.strip()]

    valid_urls = []
    invalid_rows = []

    for raw in raw_urls:
        url = normalize_url(raw)

        if url is None:
            invalid_rows.append({
                "url": raw,
                "status": "INVALID_URL",
                "status_code": "",
                "final_url": "",
                "error": "Invalid URL or hostname",
            })
        else:
            valid_urls.append(url)

    # Remove duplicates while preserving order.
    valid_urls = list(dict.fromkeys(valid_urls))

    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    headers = {"User-Agent": "BulkURLChecker/1.0"}
    results = []

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
    ) as session:
        tasks = [
            check_url(session, semaphore, url)
            for url in valid_urls
        ]

        # Collect every result before closing the session.
        completed = await asyncio.gather(
            *tasks, return_exceptions=True
        )

        for index, item in enumerate(completed):
            if isinstance(item, Exception):
                item = {
                    "url": valid_urls[index],
                    "status": "CHECK_ERROR",
                    "status_code": "",
                    "final_url": "",
                    "error": type(item).__name__,
                }

            results.append(item)
            print(
                f"[{index + 1}/{len(valid_urls)}] "
                f"{item['status']} {item['url']}"
            )

    groups = defaultdict(list)

    for result in results:
        groups[result["status"]].append(result)

    for status, rows in groups.items():
        save_csv(f"{status}.csv", rows)
        print(f"Saved {status}.csv: {len(rows)} URLs")

    if invalid_rows:
        save_csv("INVALID_URL.csv", invalid_rows)
        print(f"Saved INVALID_URL.csv: {len(invalid_rows)} URLs")

    print(f"\nComplete: {len(results)} valid URLs checked")
    print(f"Invalid input URLs: {len(invalid_rows)}")


if __name__ == "__main__":
    asyncio.run(main())
