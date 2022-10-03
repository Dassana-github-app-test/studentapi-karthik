package org.studentdept;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;

public class StudentPojo {

    private Long id;
    private String name;
    private Double marks;


    private Boolean istopper;

    public StudentPojo(Long id, String name, Double marks, Boolean istopper) {
        this.id = id;
        this.name = name;
        this.marks = marks;
        this.istopper = istopper;
    }

    public static Uni<StudentPojo> getStudentData(PgPool client, int id) {
        return client.preparedQuery("SELECT * FROM STUDENTS_DB WHERE ID = $1")
                .execute(Tuple.of(id)).onItem()
                .transform(rows -> {
                    if(rows.iterator().hasNext()) {
                        return from(rows.iterator().next());
                    }
                    throw new RuntimeException("Id not found");
                });
    }

    public static Uni<Void> update(PgPool client, Double marks, Long id) {
        return client.preparedQuery("UPDATE STUDENTS_DB SE marks =$1 WHERE id = $2")
               .execute(Tuple.of(marks,id)).onItem()//.transform(i-> Uni.createFrom().voidItem())
                .transformToUni(i -> Uni.createFrom().voidItem());
               //.transform(rows -> rows.iterator().hasNext()? from(rows.iterator().next()): null);
    }

    public static Uni<Void> addNew(PgPool client,String name, Double marks, Boolean gettopper) {
        return client.preparedQuery("INSERT INTO STUDENTS_DB(name,marks,istopper) VALUES($1,$2,$3)").execute(Tuple.of(name,marks,gettopper))
                .onItem().transformToUni(i -> Uni.createFrom().voidItem());
    }

    public static Uni<Void> removeStudent(PgPool client, int id) {
        return client.preparedQuery("DELETE FROM STUDENTS_DB WHERE id=$1")
                .execute(Tuple.of(id))
                .onItem().transformToUni(i -> Uni.createFrom().voidItem());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getMarks() {
        return marks;
    }

    public void setMarks(Double marks) {
        this.marks = marks;
    }

    public Boolean gettopper() {
        return istopper;
    }

    public void settopper(Boolean topper) {
        istopper = topper;
    }

    public static Multi<StudentPojo> getData(PgPool client){
        return  client.query("SELECT * from STUDENTS_DB ORDER BY id")
                .execute()
                .onItem()
                .transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem()
                .transform(StudentPojo::from);
    }

    private static StudentPojo from(Row row) {
        return new StudentPojo(row.getLong("id"),row.getString("name"),row.getDouble("marks"),row.getBoolean("istopper"));
    }


}
