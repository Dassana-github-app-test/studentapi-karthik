package org.studentdept;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.cli.annotations.ParsedAsList;
import io.vertx.mutiny.pgclient.PgPool;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.sql.SQLDataException;
import java.util.function.Consumer;

import static io.smallrye.mutiny.unchecked.Unchecked.*;


@Path("students")
public class StudentsDbConnection {

    @Inject
    PgPool client;

    public void preMatchingFilter(ContainerRequestContext requestContext) {
        // make sure we don't lose cheese lovers
        if("yes".equals(requestContext.getHeaderString("Cheese"))) {
            requestContext.setRequestUri(URI.create("/cheese"));
        }
    }
    @PostConstruct
    void config() {
        init_db();
    }

    private void init_db() {
        client.query("DROP TABLE IF EXISTS students_db").execute()
                .flatMap(q -> client.query("CREATE TABLE STUDENTS_DB (id SERIAL PRIMARY KEY, name TEXT NOT NULL, marks DECIMAL NOT NULL , istopper BOOLEAN NOT NULL)").execute())
                .flatMap(q -> client.query("INSERT INTO STUDENTS_DB (name,marks,istopper) VALUES ('karthik',79.5,FALSE)").execute())
                .flatMap(q -> client.query("INSERT INTO STUDENTS_DB (name,marks,istopper) VALUES ('rohit',88.5,TRUE)").execute())
                .flatMap(q -> client.query("INSERT INTO STUDENTS_DB (name,marks,istopper) VALUES ('raj manohar reddy',97.5,TRUE)").execute())
                .await().indefinitely();
    }


    @GET
    public Multi<StudentPojo> getAllData() {
        return StudentPojo.getData(client);
    }

    @GET
    @Path("{id}")
    public Uni<Response> getStudentData(@PathParam("id") int id) {
        return StudentPojo.getStudentData(client, id)
                .onItem()
                .transform(studentPojo -> studentPojo!=null?Response.ok(studentPojo):Response.status(Response.Status.NOT_FOUND))
                .onFailure().invoke(failure-> System.out.println("Can't Get the Data back"))
                .onItem().transform(Response.ResponseBuilder::build).onItem().invoke(i -> System.out.println("Received item " + i));
    }

    @PUT
    @Path("update")
    public Uni<Response> updateStudentMarks(StudentPojo studentPojo) {
        return StudentPojo.update(client, studentPojo.getMarks(), studentPojo.getId())
                .onItem().transform(i -> Response.ok().build())
                .onFailure().invoke(failure -> System.out.println(failure));
    }

    @POST
    public Uni<Response> addNewStudent(StudentPojo studentPojo) {
        return StudentPojo.addNew(client, studentPojo.getName(), studentPojo.getMarks(), studentPojo.gettopper())
                .onItem().transform(i -> Response.ok().build());
    }
    @DELETE
    @Path("remove/{id}")
    public Uni<Response> deleteExistingStudent(@PathParam("id") int id){
        return StudentPojo.removeStudent(client,id)
                .onItem().transform(i -> Response.ok().build());
    }
    @Path("/hey")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response giveResponse(){
        return Response.ok(12233).build();
    }
}
