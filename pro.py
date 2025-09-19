import argparse
import csv
import datetime
import os
import sqlite3
import sys
from typing import Iterable, List, Optional, Tuple


DB_FILE_DEFAULT = os.path.join(os.path.expanduser("~"), "attendance.db")


def get_connection(db_file: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_file)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            roll TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            email TEXT
        );

        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            teacher TEXT
        );

        CREATE TABLE IF NOT EXISTS enrollments (
            student_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            PRIMARY KEY (student_id, course_id),
            FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
            FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER NOT NULL,
            session_date TEXT NOT NULL, -- YYYY-MM-DD
            start_time TEXT,            -- HH:MM
            end_time TEXT,              -- HH:MM
            topic TEXT,
            UNIQUE(course_id, session_date, start_time),
            FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS attendance (
            session_id INTEGER NOT NULL,
            student_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('P','A','L')),
            marked_at TEXT NOT NULL,
            PRIMARY KEY (session_id, student_id),
            FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_students_roll ON students(roll);
        CREATE INDEX IF NOT EXISTS idx_courses_code ON courses(code);
        CREATE INDEX IF NOT EXISTS idx_sessions_course_date ON sessions(course_id, session_date);
        CREATE INDEX IF NOT EXISTS idx_attendance_session ON attendance(session_id);
        """
    )
    connection.commit()


def parse_date(value: str) -> str:
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")


def parse_time(value: str) -> str:
    try:
        datetime.datetime.strptime(value, "%H:%M")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError("Time must be in HH:MM 24h format")


def ensure_course_session(
    connection: sqlite3.Connection,
    course_code: str,
    session_date: str,
    start_time: Optional[str],
    end_time: Optional[str],
    topic: Optional[str],
) -> int:
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM courses WHERE code = ?", (course_code,))
    row = cursor.fetchone()
    if row is None:
        raise SystemExit(f"Course not found: {course_code}")
    course_id = row[0]

    cursor.execute(
        """
        INSERT OR IGNORE INTO sessions(course_id, session_date, start_time, end_time, topic)
        VALUES(?,?,?,?,?)
        """,
        (course_id, session_date, start_time, end_time, topic),
    )
    connection.commit()
    cursor.execute(
        "SELECT id FROM sessions WHERE course_id=? AND session_date=? AND start_time IS ?",
        (course_id, session_date, start_time),
    )
    session_id = cursor.fetchone()[0]
    return session_id


def add_student(connection: sqlite3.Connection, roll: str, name: str, email: Optional[str]) -> None:
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO students(roll, name, email) VALUES(?,?,?)",
        (roll, name, email),
    )
    connection.commit()


def add_course(connection: sqlite3.Connection, code: str, title: str, teacher: Optional[str]) -> None:
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO courses(code, title, teacher) VALUES(?,?,?)",
        (code, title, teacher),
    )
    connection.commit()


def enroll_student(connection: sqlite3.Connection, roll: str, course_code: str) -> None:
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM students WHERE roll=?", (roll,))
    student = cursor.fetchone()
    if student is None:
        raise SystemExit(f"Student not found: {roll}")
    student_id = student[0]
    cursor.execute("SELECT id FROM courses WHERE code=?", (course_code,))
    course = cursor.fetchone()
    if course is None:
        raise SystemExit(f"Course not found: {course_code}")
    course_id = course[0]
    cursor.execute(
        "INSERT OR IGNORE INTO enrollments(student_id, course_id) VALUES(?,?)",
        (student_id, course_id),
    )
    connection.commit()


def mark_attendance(
    connection: sqlite3.Connection,
    course_code: str,
    session_date: str,
    marks: Iterable[Tuple[str, str]],
    start_time: Optional[str],
    end_time: Optional[str],
    topic: Optional[str],
) -> None:
    session_id = ensure_course_session(
        connection=connection,
        course_code=course_code,
        session_date=session_date,
        start_time=start_time,
        end_time=end_time,
        topic=topic,
    )
    cursor = connection.cursor()
    now_iso = datetime.datetime.now().isoformat(timespec="seconds")

    # Resolve all rolls to student ids, and validate enrollment
    rolls = [roll for roll, _ in marks]
    placeholders = ",".join(["?"] * len(rolls)) if rolls else ""
    roll_to_id = {}
    if rolls:
        cursor.execute(
            f"SELECT id, roll FROM students WHERE roll IN ({placeholders})",
            rolls,
        )
        for row in cursor.fetchall():
            roll_to_id[row[1]] = row[0]

    cursor.execute("SELECT id FROM courses WHERE code=?", (course_code,))
    course_row = cursor.fetchone()
    if course_row is None:
        raise SystemExit(f"Course not found: {course_code}")
    course_id = course_row[0]

    for roll, status in marks:
        if status not in ("P", "A", "L"):
            raise SystemExit("Status must be one of P, A, L")
        student_id = roll_to_id.get(roll)
        if student_id is None:
            raise SystemExit(f"Unknown student roll: {roll}")
        # Ensure enrollment
        cursor.execute(
            "SELECT 1 FROM enrollments WHERE student_id=? AND course_id=?",
            (student_id, course_id),
        )
        if cursor.fetchone() is None:
            raise SystemExit(f"Student {roll} not enrolled in {course_code}")
        cursor.execute(
            """
            INSERT INTO attendance(session_id, student_id, status, marked_at)
            VALUES(?,?,?,?)
            ON CONFLICT(session_id, student_id) DO UPDATE SET
                status=excluded.status,
                marked_at=excluded.marked_at
            """,
            (session_id, student_id, status, now_iso),
        )
    connection.commit()


def fetch_course_stats(connection: sqlite3.Connection, course_code: str) -> Tuple[int, int, List[Tuple[str, str, int, int, int, float]]]:
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM courses WHERE code=?", (course_code,))
    course = cursor.fetchone()
    if course is None:
        raise SystemExit(f"Course not found: {course_code}")
    course_id = course[0]

    cursor.execute("SELECT COUNT(*) FROM sessions WHERE course_id=?", (course_id,))
    total_sessions = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT s.roll, s.name,
               SUM(CASE WHEN a.status='P' THEN 1 ELSE 0 END) AS present_count,
               SUM(CASE WHEN a.status='A' THEN 1 ELSE 0 END) AS absent_count,
               SUM(CASE WHEN a.status='L' THEN 1 ELSE 0 END) AS late_count
        FROM students s
        JOIN enrollments e ON e.student_id = s.id AND e.course_id = ?
        LEFT JOIN attendance a ON a.student_id = s.id
        LEFT JOIN sessions se ON se.id = a.session_id AND se.course_id = ?
        GROUP BY s.id
        ORDER BY s.roll
        """,
        (course_id, course_id),
    )
    rows = cursor.fetchall()

    results: List[Tuple[str, str, int, int, int, float]] = []
    for roll, name, present, absent, late in rows:
        present = present or 0
        absent = absent or 0
        late = late or 0
        attended = present + late
        percentage = (attended / total_sessions * 100.0) if total_sessions > 0 else 0.0
        results.append((roll, name, present, absent, late, round(percentage, 2)))

    return total_sessions, len(rows), results


def report_course(connection: sqlite3.Connection, course_code: str) -> None:
    total_sessions, total_students, rows = fetch_course_stats(connection, course_code)
    print(f"Course {course_code} - Students: {total_students}, Sessions: {total_sessions}")
    print("Roll, Name, Present, Absent, Late, Percentage")
    for roll, name, present, absent, late, percentage in rows:
        print(f"{roll}, {name}, {present}, {absent}, {late}, {percentage:.2f}")


def report_student(connection: sqlite3.Connection, roll: str) -> None:
    cursor = connection.cursor()
    cursor.execute("SELECT id, name FROM students WHERE roll=?", (roll,))
    student = cursor.fetchone()
    if student is None:
        raise SystemExit(f"Student not found: {roll}")
    student_id, name = student
    cursor.execute(
        """
        SELECT c.code,
               SUM(CASE WHEN a.status='P' THEN 1 ELSE 0 END) AS present_count,
               SUM(CASE WHEN a.status='A' THEN 1 ELSE 0 END) AS absent_count,
               SUM(CASE WHEN a.status='L' THEN 1 ELSE 0 END) AS late_count,
               (SELECT COUNT(*) FROM sessions s WHERE s.course_id = c.id) AS total_sessions
        FROM courses c
        JOIN enrollments e ON e.course_id = c.id AND e.student_id = ?
        LEFT JOIN sessions se ON se.course_id = c.id
        LEFT JOIN attendance a ON a.session_id = se.id AND a.student_id = ?
        GROUP BY c.id
        ORDER BY c.code
        """,
        (student_id, student_id),
    )
    print(f"Student {roll} - {name}")
    print("Course, Present, Absent, Late, Percentage")
    for code, present, absent, late, total_sessions in cursor.fetchall():
        present = present or 0
        absent = absent or 0
        late = late or 0
        attended = present + late
        percentage = (attended / total_sessions * 100.0) if total_sessions > 0 else 0.0
        print(f"{code}, {present}, {absent}, {late}, {percentage:.2f}")


def report_daily(connection: sqlite3.Connection, session_date: str, course_code: Optional[str]) -> None:
    cursor = connection.cursor()
    if course_code:
        cursor.execute("SELECT id FROM courses WHERE code=?", (course_code,))
        course = cursor.fetchone()
        if course is None:
            raise SystemExit(f"Course not found: {course_code}")
        course_id = course[0]
        cursor.execute(
            """
            SELECT c.code, se.id, se.start_time, se.topic, s.roll, s.name, a.status
            FROM sessions se
            JOIN courses c ON c.id = se.course_id
            LEFT JOIN attendance a ON a.session_id = se.id
            LEFT JOIN students s ON s.id = a.student_id
            WHERE se.session_date = ? AND se.course_id = ?
            ORDER BY c.code, se.start_time, s.roll
            """,
            (session_date, course_id),
        )
    else:
        cursor.execute(
            """
            SELECT c.code, se.id, se.start_time, se.topic, s.roll, s.name, a.status
            FROM sessions se
            JOIN courses c ON c.id = se.course_id
            LEFT JOIN attendance a ON a.session_id = se.id
            LEFT JOIN students s ON s.id = a.student_id
            WHERE se.session_date = ?
            ORDER BY c.code, se.start_time, s.roll
            """,
            (session_date,),
        )
    print("Course, SessionID, StartTime, Topic, Roll, Name, Status")
    for code, sid, start_time, topic, roll, name, status in cursor.fetchall():
        roll = roll or ""
        name = name or ""
        status = status or ""
        topic = topic or ""
        start_time = start_time or ""
        print(f"{code}, {sid}, {start_time}, {topic}, {roll}, {name}, {status}")


def export_csv(connection: sqlite3.Connection, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    cursor = connection.cursor()

    def write_csv(filename: str, headers: List[str], rows: Iterable[Tuple]) -> None:
        path = os.path.join(output_dir, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)

    for name, query, headers in [
        (
            "students.csv",
            "SELECT id, roll, name, email FROM students ORDER BY roll",
            ["id", "roll", "name", "email"],
        ),
        (
            "courses.csv",
            "SELECT id, code, title, teacher FROM courses ORDER BY code",
            ["id", "code", "title", "teacher"],
        ),
        (
            "enrollments.csv",
            "SELECT student_id, course_id FROM enrollments ORDER BY course_id, student_id",
            ["student_id", "course_id"],
        ),
        (
            "sessions.csv",
            "SELECT id, course_id, session_date, start_time, end_time, topic FROM sessions ORDER BY session_date, course_id",
            ["id", "course_id", "session_date", "start_time", "end_time", "topic"],
        ),
        (
            "attendance.csv",
            "SELECT session_id, student_id, status, marked_at FROM attendance ORDER BY session_id, student_id",
            ["session_id", "student_id", "status", "marked_at"],
        ),
    ]:
        cursor.execute(query)
        write_csv(name, headers, cursor.fetchall())
    print(f"Exported CSVs to {output_dir}")


def import_csv(connection: sqlite3.Connection, input_dir: str) -> None:
    cursor = connection.cursor()
    path = os.path.join(input_dir, "students.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cursor.execute(
                    "INSERT OR REPLACE INTO students(id, roll, name, email) VALUES(?,?,?,?)",
                    (row.get("id"), row["roll"], row["name"], row.get("email")),
                )
    path = os.path.join(input_dir, "courses.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cursor.execute(
                    "INSERT OR REPLACE INTO courses(id, code, title, teacher) VALUES(?,?,?,?)",
                    (row.get("id"), row["code"], row["title"], row.get("teacher")),
                )
    path = os.path.join(input_dir, "enrollments.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cursor.execute(
                    "INSERT OR IGNORE INTO enrollments(student_id, course_id) VALUES(?,?)",
                    (row["student_id"], row["course_id"]),
                )
    path = os.path.join(input_dir, "sessions.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cursor.execute(
                    "INSERT OR REPLACE INTO sessions(id, course_id, session_date, start_time, end_time, topic) VALUES(?,?,?,?,?,?)",
                    (
                        row.get("id"),
                        row["course_id"],
                        row["session_date"],
                        row.get("start_time"),
                        row.get("end_time"),
                        row.get("topic"),
                    ),
                )
    path = os.path.join(input_dir, "attendance.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cursor.execute(
                    "INSERT OR REPLACE INTO attendance(session_id, student_id, status, marked_at) VALUES(?,?,?,?)",
                    (row["session_id"], row["student_id"], row["status"], row["marked_at"]),
                )
    connection.commit()
    print(f"Imported CSVs from {input_dir}")


def seed_sample_data(connection: sqlite3.Connection) -> None:
    add_student(connection, "CS001", "Alice Johnson", "alice@example.com")
    add_student(connection, "CS002", "Bob Smith", "bob@example.com")
    add_student(connection, "CS003", "Charlie Lee", "charlie@example.com")

    add_course(connection, "CSE101", "Intro to CS", "Dr. Gupta")
    add_course(connection, "MAT201", "Discrete Math", "Dr. Rao")

    enroll_student(connection, "CS001", "CSE101")
    enroll_student(connection, "CS002", "CSE101")
    enroll_student(connection, "CS003", "CSE101")
    enroll_student(connection, "CS001", "MAT201")
    enroll_student(connection, "CS003", "MAT201")

    today = datetime.date.today().strftime("%Y-%m-%d")
    mark_attendance(
        connection,
        course_code="CSE101",
        session_date=today,
        marks=[("CS001", "P"), ("CS002", "A"), ("CS003", "L")],
        start_time="09:00",
        end_time="10:00",
        topic="Introduction",
    )
    mark_attendance(
        connection,
        course_code="MAT201",
        session_date=today,
        marks=[("CS001", "P"), ("CS003", "P")],
        start_time="11:00",
        end_time="12:00",
        topic="Sets",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automated Student Attendance Monitoring and Analytics (SQLite CLI)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=DB_FILE_DEFAULT, help="SQLite database file path")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize database schema")

    p_add_student = sub.add_parser("add-student", help="Add a student")
    p_add_student.add_argument("roll")
    p_add_student.add_argument("name")
    p_add_student.add_argument("--email")

    p_add_course = sub.add_parser("add-course", help="Add a course")
    p_add_course.add_argument("code")
    p_add_course.add_argument("title")
    p_add_course.add_argument("--teacher")

    p_enroll = sub.add_parser("enroll", help="Enroll a student to a course")
    p_enroll.add_argument("roll")
    p_enroll.add_argument("course_code")

    p_mark = sub.add_parser("mark", help="Mark attendance for a course session")
    p_mark.add_argument("course_code")
    p_mark.add_argument("date", type=parse_date)
    p_mark.add_argument("marks", nargs="+", help="Pairs like ROLL:P|A|L, e.g., CS001:P")
    p_mark.add_argument("--start", type=parse_time)
    p_mark.add_argument("--end", type=parse_time)
    p_mark.add_argument("--topic")

    p_report_course = sub.add_parser("report-course", help="Course attendance stats")
    p_report_course.add_argument("course_code")

    p_report_student = sub.add_parser("report-student", help="Student attendance across courses")
    p_report_student.add_argument("roll")

    p_report_daily = sub.add_parser("report-daily", help="Daily session log, optionally filtered by course")
    p_report_daily.add_argument("date", type=parse_date)
    p_report_daily.add_argument("--course")

    p_export = sub.add_parser("export-csv", help="Export tables to CSV files in a folder")
    p_export.add_argument("output_dir")

    p_import = sub.add_parser("import-csv", help="Import tables from CSV files in a folder")
    p_import.add_argument("input_dir")

    sub.add_parser("seed", help="Insert sample data for quick testing")

    return parser


def parse_marks_args(marks_args: List[str]) -> List[Tuple[str, str]]:
    parsed: List[Tuple[str, str]] = []
    for token in marks_args:
        if ":" not in token:
            raise SystemExit("Invalid mark format. Use ROLL:STATUS, e.g., CS001:P")
        roll, status = token.split(":", 1)
        status = status.upper()
        parsed.append((roll, status))
    return parsed


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    connection = get_connection(args.db)

    if args.command == "init-db":
        initialize_database(connection)
        print(f"Initialized database at {args.db}")
        return 0

    # Ensure schema exists for all other commands
    initialize_database(connection)

    if args.command == "add-student":
        add_student(connection, args.roll, args.name, args.email)
        print(f"Added student {args.roll} - {args.name}")
        return 0

    if args.command == "add-course":
        add_course(connection, args.code, args.title, args.teacher)
        print(f"Added course {args.code} - {args.title}")
        return 0

    if args.command == "enroll":
        enroll_student(connection, args.roll, args.course_code)
        print(f"Enrolled {args.roll} to {args.course_code}")
        return 0

    if args.command == "mark":
        marks = parse_marks_args(args.marks)
        mark_attendance(
            connection,
            course_code=args.course_code,
            session_date=args.date,
            marks=marks,
            start_time=args.start,
            end_time=args.end,
            topic=args.topic,
        )
        print("Attendance saved")
        return 0

    if args.command == "report-course":
        report_course(connection, args.course_code)
        return 0

    if args.command == "report-student":
        report_student(connection, args.roll)
        return 0

    if args.command == "report-daily":
        report_daily(connection, args.date, args.course)
        return 0

    if args.command == "export-csv":
        export_csv(connection, args.output_dir)
        return 0

    if args.command == "import-csv":
        import_csv(connection, args.input_dir)
        return 0

    if args.command == "seed":
        seed_sample_data(connection)
        print("Seeded sample data")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except sqlite3.IntegrityError as exc:
        print(f"Integrity error: {exc}")
        sys.exit(2)
    except SystemExit as exc:
        # Re-raise to respect exit code and message already printed
        raise
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        sys.exit(1)


