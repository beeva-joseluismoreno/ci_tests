package com.bbva.kbdp.flume.source.interceptor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class CustomSourceInterceptor implements Interceptor, Interceptor.Builder {

	private CustomSourceInterceptor interceptor;

	@Override
	public void initialize() {
		System.out.println("INTERCEPTOR.INITIALIZE :: ");
		// TODO Auto-generated method stub

	}

	@Override
	public Event intercept(Event event) {

		//System.out.println("INTERCEPTOR.INTERCEPT :: ");
		int module = event.hashCode() % 5;

		Map<String, String> headers = event.getHeaders();
		headers.put("partition", "A" + module);
		//System.out.println("PARTITION ::::::::::::::::::: " +  "A" + module);

		event.setHeaders(headers);

		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		//System.out.println("INTERCEPTOR.INTERCEPT LIST:: ");
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
			Event next = intercept(iterator.next());
			if (next == null) {
				iterator.remove();
			}
		}
		return events;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(Context context) {
		System.out.println("INTERCEPTOR.CONFIGURE :: ");
		interceptor = new CustomSourceInterceptor();

	}

	@Override
	public Interceptor build() {
		System.out.println("INTERCEPTOR.BUILD :: ");
		return interceptor;
	}

}
