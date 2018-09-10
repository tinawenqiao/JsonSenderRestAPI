package org.cmbc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;
import org.hibernate.validator.internal.engine.path.PathImpl;
import org.springframework.http.HttpStatus;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;

import javax.validation.ConstraintViolation;
import java.time.LocalDateTime;
import java.util.*;

@Getter
@Setter
@AllArgsConstructor
public class RestResult {
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;
  public HttpStatus status;
  public int code;
  public String msg;
  public Object data;
  public Object extra;

  public RestResult() {
    this.timestamp = LocalDateTime.now();
    this.data = new Object();
  }

  public RestResult(HttpStatus httpStatus, String msg, Throwable ex) {
    this();
    this.setStatus(httpStatus);
    this.code = httpStatus.value();
    this.msg = msg;
    Map errorMsg = new HashMap();
    errorMsg.put("error", ex.getLocalizedMessage());
    this.setData(errorMsg);
  }

  public static RestResult success(Object data, String msg) {
    RestResult result = new RestResult();
    result.setStatus(HttpStatus.OK);
    result.setCode(ResultCode.SUCCESS.code());
    result.setData(data);
    result.setMsg(msg);
    return result;
  }

  public static RestResult failure(ResultCode resultCode, String msg, Object data) {
    RestResult result = new RestResult();
    result.setCode(resultCode.code());
    result.setData(data);
    result.setMsg(msg);
    return result;
  }

  public void addValidationErrors(List<FieldError> fieldErrors) {
    fieldErrors.forEach(this::addValidationError);
  }

  public void addValidationErrors(Set<ConstraintViolation<?>> constraintViolations) {
    constraintViolations.forEach(this::addValidationError);
  }

  private void addValidationError(ObjectError objectError) {
    this.addValidationError(
            objectError.getObjectName(),
            objectError.getDefaultMessage());
  }

  private void addValidationError(String object, String message) {
    addSubError(new RestValidationError(object, message));
  }

  public void addValidationError(List<ObjectError> globalErrors) {
    globalErrors.forEach(this::addValidationError);
  }

  /**
   * Utility method for adding error of ConstraintViolation. Usually when a @Validated validation fails.
   * @param cv the ConstraintViolation
   */
  private void addValidationError(ConstraintViolation<?> cv) {
    this.addValidationError(
            cv.getRootBeanClass().getSimpleName(),
            ((PathImpl) cv.getPropertyPath()).getLeafNode().asString(),
            cv.getInvalidValue(),
            cv.getMessage());
  }

  private void addValidationError(String object, String field, Object rejectedValue, String message) {
    addSubError(new RestValidationError(object, field, rejectedValue, message));
  }

  private void addSubError(RestSubError subError) {
    ArrayList subErrorList = new ArrayList<>();
    if (((Map)this.data).get("subErrorList") != null) {
      subErrorList = (ArrayList) ((Map)this.data).get("subErrorList");
    }
    subErrorList.add(subError);
    ((Map)this.data).put("subErrorList", subErrorList);
  }

  abstract class RestSubError {

  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  @AllArgsConstructor
  class RestValidationError extends RestSubError {
    private String object;
    private String field;
    private Object rejectedValue;
    private String message;

    RestValidationError(String object, String message) {
      this.object = object;
      this.message = message;
    }
  }
}
