SELECT  s.fname as FirstName, 
        s.lname as LastName, 
        c.CourseName 
FROM students s 
JOIN student_courses sc 
ON s.id = sc.student_id 
JOIN course c 
ON c.id = sc.course_id
